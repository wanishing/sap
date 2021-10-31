#!/usr/bin/env bb
(ns sap
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [cl-format]]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml]
            [clojure.core.async :as async]
            [babashka.wait :as wait]
            [babashka.curl :as curl]
            [babashka.process :refer [process]]
            [clojure.tools.cli :refer [parse-opts]])
  (:import [java.net Socket SocketException]))

(def ^:dynamic *verbose* nil)

(defn- run-sh [& args]
  (when *verbose*
    (println (str/join " " args)))
  (let [{:keys [out exit err]} (apply sh args)]
    (if (zero? exit)
      out
      (throw (ex-info err {:command args :error err})))))

(defn- run-proc [& args]
  (when *verbose*
    (println (str/join " " args)))
  @(process args {:out :inherit})
  nil)

(defn- print-rows [rows]
  (when (seq rows)
    (let [divider (apply str (repeat 4 " "))
          cols (keys (first rows))
          headers (map #(str/upper-case (name %)) cols)
          widths (map
                  (fn [k]
                    (apply max (count (str k)) (map #(count (str (get % k))) rows)))
                  cols)
          fmts (map #(str "%-" % "s") widths)
          fmt-row (fn [row]
                    (apply str (interpose divider
                                          (for [[col fmt] (map vector (map #(get row %) cols) fmts)]
                                            (format fmt (str col))))))]
      (println (fmt-row (zipmap cols headers)))
      (doseq [row rows]
        (println (fmt-row row))))))

(defn- driver [id]
  (format "%s-driver" id))

(defn- now [] (java.time.Instant/now))

(defn- to-inst [s] (java.time.Instant/parse s))

(defn- at-utc [inst] (.atZone inst java.time.ZoneOffset/UTC))

(defn- duration [start end]
  (let [diff (java.time.Duration/between end start)
        units [[(.toDays diff) "d"]
               [(mod (.toHours diff) 24) "h"]
               [(mod (.toMinutes diff) 60) "m"]
               [(mod (.toSeconds diff) 60) "s"]]
        result (->> units
                    (filter (fn [[diff _]] (pos? diff)))
                    (map (fn [[diff unit]] (format "%d%s" diff unit)))
                    (str/join))]
    result))

(defn- parse-app [[id created-at state terminated-at]]
  {:id id
   :state (or state "UNKNOWN")
   :created-at created-at
   :terminated-at (if (= terminated-at "<nil>") nil terminated-at)
   :age (duration (now) (to-inst created-at))})

(defn- jsonpath [fields]
  (let [formatted-fields (->> fields
                              (map #(format "{%s}" %))
                              (str/join "{\"\\t\"}"))
        jsonpath (format "-o=jsonpath={range .items[*]}%s{\"\\n\"}{end}" formatted-fields)]
    jsonpath))

(defn- spark-apps
  ([state]
   (if-let [given-state (and (some? state) (str/upper-case state))]
     (filter (fn [{:keys [state]}]
               (= state given-state)) (spark-apps))
     (spark-apps)))
  ([]
   (let [raw-apps (run-sh "kubectl" "get" "sparkapplication" (jsonpath
                                                              [".metadata.name"
                                                               ".metadata.creationTimestamp"
                                                               ".status.applicationState.state"
                                                               ".status.terminationTime"]))
         apps (->> raw-apps
                   (str/split-lines)
                   (filter #(not (str/blank? %)))
                   (map #(str/split % #"\t"))
                   (map parse-app))]
     apps)))

(defn- forward-port [driver port]
  (run-sh "kubectl" "port-forward" driver (format "%s:4040" port)))

(defn- busy? [port]
  (try
    (nil? (.close (Socket. "localhost" port)))
    (catch SocketException _
      false)))

(defn- find-free-port []
  (let [start 4040
        end (+ start 50)]
    (loop [port start]
      (cond
        (>= port end) nil
        (not (busy? port)) port
        :else (recur (inc port))))))

(defn- spark-ui [{:keys [id]} _]
  (let [driver-app (driver id)
        port (find-free-port)]
    (println "Port forwarding" driver-app
             (format "to http://localhost:%s..." port))
    (forward-port driver-app port)))

(defn- localhost [port]
  (format "localhost:%s" port))

(defn- get-by [api]
  (let [headers {"Accept" "application/json"}
        resp (curl/get api {:headers headers
                            :throw false})
        ->snakecase (fn [s]
                      (let [end (count s)
                            appender (fn [idx]
                                       (let [c (nth s idx)]
                                         (if (java.lang.Character/isUpperCase c)
                                           (str "-" (str/lower-case c))
                                           c)))]
                        (loop [curr 0
                               acc ""]
                          (if (= end curr)
                            acc
                            (recur (inc curr) (str acc (appender curr)))))))
        body (when (= (:status resp) 200)
               (-> resp
                   :body
                   (json/parse-string (comp keyword ->snakecase))))]
    (when *verbose*
      (println api "=>" (json/parse-string body {:pretty true})))
    body))

(defn- ->millis [millis]
  (if (or (nil? millis) (zero? millis))
    "0.0ms"
    (let [inst (at-utc (java.time.Instant/ofEpochMilli millis))
          units [[(.getHour inst) "h"]
                 [(.getMinute inst) "m"]
                 [(.getSecond inst) "s"]
                 [(int (mod (Math/round millis) 1000)) "ms"]]
          res (->> units
                   (filter (fn [[val _]] (pos? val)))
                   (map (fn [[val unit]] (format "%d%s" val unit)))
                   (str/join))]
      res)))

(defn- ->bytes [bytes]
  (if (some? bytes)
    (let [units [[(double (/ bytes (* 1024 1024 1024))) "GiB"]
                 [(double (/ bytes (* 1024 1024))) "MiB"]
                 [(double (/ bytes 1024)) "KiB"]
                 [bytes "B"]]
          [val unit] (some (fn [[val unit]]
                             (and (>= val 1) [val unit])) units)
          res (format "%.1f%s" val unit)]
      res)
    0.0))

(defn- fetch-app [endpoint]
  (let [api (str endpoint "/api/v1/applications")
        {:keys [id]} (first (get-by api))]
    id))

(defn- bytes-per-records [bytes records]
  (when (every? pos? [bytes records])
    (format "%s/%s" (->bytes bytes) (cl-format nil "~:d" (long records)))))

(defn- fetch-metrics [endpoint app-id stage-id attempt-id]
  (let [quantiles [0.01 0.25 0.5 0.75 0.99]
        quantiles-names ["Min" "25th" "Median" "75th" "Max"]
        metrics-api (str endpoint (format "/api/v1/applications/%s/stages/%s/%s/taskSummary?quantiles=%s"
                                          app-id
                                          stage-id
                                          attempt-id
                                          (str/join "," quantiles)))
        {:keys [executor-run-time jvm-gc-time] :as response
         {:keys [bytes-read records-read]} :input-metrics
         {:keys [bytes-written records-written]} :output-metrics
         {:keys [read-bytes read-records]} :shuffle-read-metrics
         {:keys [write-bytes write-records]} :shuffle-write-metrics} (get-by metrics-api)
        metrics (if (some? response)
                  (map-indexed (fn [idx quantile]
                                 (let [nth #(nth % idx)]
                                   {:percentile quantile
                                    :duration (->millis (nth executor-run-time))
                                    :gc-time (->millis (nth jvm-gc-time))
                                    :input (bytes-per-records (nth bytes-read) (nth records-read))
                                    :output (bytes-per-records (nth bytes-written) (nth records-written))
                                    :shuffle-read (bytes-per-records (nth read-bytes) (nth read-records))
                                    :shuffle-write (bytes-per-records (nth write-bytes) (nth write-records))}))
                               quantiles-names)
                  [])
        nullable-fields (filter (fn [field]
                                  (every? nil? (map field metrics)))
                                [:input :output :shuffle-read :shuffle-write])
        metrics (map (fn [m]
                       (apply (partial dissoc m) nullable-fields)) metrics)]
    metrics))

(defn- fetch-stage [endpoint id]
  (let [stages-api (str endpoint (format "/api/v1/applications/%s/stages?status=active" id))
        stages (get-by stages-api)]
    (when (seq stages)
      (let [{:keys [stage-id
                    attempt-id
                    num-tasks
                    num-failed-tasks
                    num-active-tasks
                    num-complete-tasks
                    input-bytes
                    input-records
                    output-bytes
                    output-records
                    shuffle-write-bytes
                    shuffle-write-records
                    shuffle-read-bytes
                    shuffle-read-records
                    submission-time
                    description
                    name]} (last stages)
            stage {:app-id id
                   :stage-id stage-id
                   :attempt-id attempt-id
                   :duration (let [sbt (java.time.ZonedDateTime/parse submission-time
                                                                      (java.time.format.DateTimeFormatter/ofPattern
                                                                       "yyyy-MM-dd'T'HH:mm:ss.SSSz"))
                                   sbt (at-utc (.toInstant sbt))
                                   now (at-utc (now))]
                               (duration now sbt))
                   :description (or (and description (first (str/split-lines description))) name)
                   :input (bytes-per-records input-bytes input-records)
                   :output (bytes-per-records output-bytes output-records)
                   :shuffle-write (bytes-per-records shuffle-write-bytes shuffle-write-records)
                   :shuffle-read (bytes-per-records shuffle-read-bytes shuffle-read-records)
                   :tasks {:succeeded num-complete-tasks
                           :failed num-failed-tasks
                           :active (if (pos? num-active-tasks)
                                     num-active-tasks
                                     (- num-active-tasks))
                           :total num-tasks}}
            nullable-fields (filter #(nil? (% stage))
                                    [:input :output :shuffle-read :shuffle-write])
            stage (apply (partial dissoc stage) nullable-fields)]
        stage))))

(defn- print-metrics [{:keys [app-id
                              duration
                              stage-id
                              attempt-id
                              description
                              input
                              output
                              shuffle-write
                              shuffle-read]
                       {:keys [total active failed succeeded]} :tasks} metrics]
  (println "Application:" (str app-id))
  (println "Description:" description)
  (println "Duration:" (str duration))
  (println "Stage:" (str stage-id))
  (println "Attempt:" (str attempt-id))
  (when (some? input)
    (println (format "Input Size/Records: %s" input)))
  (when (some? output)
    (println (format "Output Size/Records: %s" output)))
  (when (some? shuffle-write)
    (println (format "Shuffle Write Size/Records: %s" shuffle-write)))
  (when (some? shuffle-read)
    (println (format "Shuffle Read Size/Records: %s" shuffle-read)))
  (println "Tasks:")
  (println (format "\tActive: %s" (str active)))
  (println (format "\tSucceeded/Total: %s/%s" (str succeeded) (str total)))
  (println (format "\tFailed: %s" (str failed)))
  (println)
  (when (some? metrics)
    (println (with-out-str (print-rows metrics)))))

(defn- clear []
  (println "\033[H\033[2J"))

(declare command-factory)

(defn- find-app
  ([partial-id]
   (find-app ((command-factory :apps)) partial-id))
  ([apps partial-id]
   (if-let [app (some (fn [{:keys [id] :as app}] (and (str/includes? id partial-id) app)) apps)]
     app
     (throw (ex-info "Failed to find app" {:id partial-id})))))

(defn- running?
  ([id]
   (let [{:keys [state]} (find-app id)]
     (= state "RUNNING")))
  ([id fetch-stage]
   (let [running? (running? id)
         active? (let [timeout (* 1000 20)
                       now #(System/currentTimeMillis)
                       seconds #(int (/ % 1000))
                       start (now)]
                   (loop [so-far 0]
                     (cond
                       (some? (fetch-stage)) true
                       (>= so-far timeout) (do
                                             (clear)
                                             (println "Timeout reached")
                                             false)
                       :else
                       (do
                         (clear)
                         (print (format "\rWaiting for active stage for %d seconds" (seconds so-far)))
                         (doseq [c (repeat 5 ".")]
                           (print c)
                           (flush)
                           (Thread/sleep 1000))
                         (recur (- (now) start))))))]
     (and running? active?))))

(defn- metrics [id]
  (when (running? id)
    (let [driver-app (driver id)
          port (find-free-port)]
      (async/thread (forward-port driver-app port))
      (wait/wait-for-port "localhost" port)
      (let [endpoint (localhost port)
            app-id (fetch-app endpoint)
            fetch-stage (partial fetch-stage endpoint app-id)
            fetch-metrics (partial fetch-metrics endpoint app-id)]
        (while (running? id fetch-stage)
          (let [{:keys [stage-id
                        attempt-id] :as stage} (fetch-stage)
                metrics (fetch-metrics stage-id attempt-id)]
            (when (some? stage)
              (clear)
              (println (format "Displaying metrics for %s (localhost:%s):" id port))
              (println)
              (print-metrics stage metrics)))
          (Thread/sleep 1000))))
    (println)
    (println (format "Application %s has no active stage" id))))

(defn- fresh-app [raw-app]
  (let [app (yaml/parse-string raw-app)
        fresh-metadata (select-keys (:metadata app) [:name :namespace])
        fresh-app (-> app
                      (assoc :metadata fresh-metadata)
                      (dissoc :status))]
    fresh-app))

(defn- yaml [id]
  (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml"))

(defn- delete [id]
  (run-proc "kubectl" "delete" "sparkapplication" id))

(defn- reapply [{:keys [id]} {:keys [image]}]
  (let [raw-app (yaml id)
        fresh-app (cond-> (fresh-app raw-app)
                    (some? image) (assoc-in [:spec :image] image))
        fresh-app (yaml/generate-string fresh-app)
        fname (format "/tmp/%s.yaml" id)]
    (spit fname fresh-app)
    (println (format "Fresh app created at %s" fname))
    (delete id)
    (run-proc "kubectl" "apply" "-f" fname)))

(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods" "metrics"})

(def command-by-name {:delete (fn [{:keys [id]} _]
                                (delete id))

                      :apps spark-apps

                      :states #{"FAILED" "COMPLETED"}

                      :ui spark-ui

                      :get (fn [{:keys [id]} {:keys [fresh]}]
                             (let [yaml (cond-> (yaml id)
                                          (some? fresh) (fresh-app))]
                               (println yaml)))

                      :desc (fn [{:keys [id]} _]
                              (run-proc "kubectl" "describe" "sparkapplication" id))

                      :reapply reapply

                      :logs (fn [{:keys [id]} _]
                              (run-proc "kubectl" "logs" "-f" (driver id)))

                      :pods (fn [{:keys [id]} _]
                              (let [label (format "sparkoperator.k8s.io/app-name=%s" id)]
                                (run-proc "kubectl" "get" "pods" "-l" label)))

                      :metrics (fn [{:keys [id]} _]
                                 (metrics id))})

(defn- command-factory [cmd]
  (get-in command-by-name [cmd]))

(defn- fetch-executors []
  (let [parse (fn [extr]
                (let [[pod, labels] (str/split extr #"\t")
                      app (-> labels
                              (json/parse-string true)
                              (:sparkoperator.k8s.io/app-name))]
                  {:executor pod :app app}))
        executors (->> (run-sh "kubectl" "get" "pods" "-l" "spark-role=executor" (jsonpath
                                                                                  [".metadata.name"
                                                                                   ".metadata.labels"]))
                       (str/split-lines)
                       (map parse))]
    executors))

(def wide-info
  (delay
   (let [add-executors (fn [executors {:keys [id] :as app}]
                         (let [pods (filter #(= id (:app %)) executors)]
                           (assoc app :executors (count pods))))
         add-duration (fn [{:keys [age created-at terminated-at] :as app}]
                        (let [duration (if (some? terminated-at)
                                         (duration (to-inst terminated-at) (to-inst created-at))
                                         age)]
                          (assoc app :duration duration)))]
     (comp
      (map add-duration)
      (map (partial add-executors (fetch-executors)))))))

(defn- find-apps-by
  [{:keys [state id days prefix wide]}]
  (let [apps (command-factory :apps)
        apps  (if (some? id)
                [(find-app (apps) id)]
                (apps state))
        older? (fn [{:keys [created-at]}]
                 (let [diff (.toDays (java.time.Duration/between (to-inst created-at) (now)))]
                   (>= diff days)))
        apps (cond->> apps
               (some? days) (filter older?)
               (some? prefix) (filter (fn [{:keys [id]}]
                                        (str/starts-with? id prefix)))
               (some? wide) (into [] @wide-info))]
    apps))

(defn- run-one [cmd options]
  (let [app (first (find-apps-by options))
        cmd (command-factory cmd)]
    (cmd app options)))

(defn- run-many [cmd options]
  (let [apps (find-apps-by options)
        cmd (command-factory cmd)
        cmds (map #(future (cmd % options)) apps)]
    (doseq [cmd cmds]
      @cmd)))

(defmulti command
  (fn [{:keys [action]}] (keyword action)))

(defmethod command :delete [{:keys [args]}]
  (run-many :delete args))

(defmethod command :cleanup [{:keys [args]}]
  (doseq [state (command-factory :states)]
    (println "Starting cleanup of" state "jobs")
    (command {:action :delete :args (assoc args :state state)})))

(defmethod command :ls [{:keys [args]}]
  (let [invisble-fields [:created-at :terminated-at]]
    (->> (find-apps-by args)
         (sort-by (juxt :id :created-at))
         (map #(reduce (fn [app key] (dissoc app key)) % invisble-fields))
         (print-rows))))

(defmethod command :pods [{:keys [args]}]
  (run-many :pods args))

(defmethod command :ui [{:keys [args]}]
  (run-one :ui args))

(defmethod command :get [{:keys [args]}]
  (run-many :get args))

(defmethod command :desc [{:keys [args]}]
  (run-many :desc args))

(defmethod command :logs [{:keys [args]}]
  (run-one :logs args))

(defmethod command :reapply [{:keys [args]}]
  (run-many :reapply args))

(defmethod command :metrics [{:keys [args]}]
  (run-one :metrics args))

(def cli-options
  [["-s" "--state STATE" "State of application"]
   ["-i" "--id ID" "Application id, can be supplied partially"]
   [nil "--days DAYES" "Minimum amount of days the application is alive"
    :parse-fn #(Integer/parseInt %)
    :default 0]
   ["-p" "--prefix PREFIX" "Prefix of application id"]
   [nil "--fresh" "When combined with `get`, the job will be displayed without runtime properties"]
   [nil "--image IMAGE" "When combined with `reapply`, the job will be re-applied with given image"]
   ["-w" "--wide"]
   ["-v" "--verbose"]
   ["-h" "--help"]])

(defn- usage [options-summary]
  (->> ["kubectl wrapper to perform common actions against Spark applications."
        ""
        "Usage: sap action [options]"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  delete        delete application"
        "  cleanup       delete COMPLETED/FAILED/Failed applications"
        "  ls            list applications"
        "  ui            port-forwarding application ui given id"
        "  get           alias for `kubectl get -o yaml` command"
        "  desc          alias for `kubectl describe` command"
        "  logs          alias for `kubectl logs` command"
        "  pods          display all pods associated to application"
        "  reapply       re-apply application (keeping the same id)"
        "  metrics       display metrics of the current active stage"
        ""]
       (str/join \newline)))

(defn- error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn- validate-args
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      {:exit-message (usage summary) :ok? true}
      errors
      {:exit-message (error-msg errors)}
      (and (= 1 (count arguments))
           (commands (first arguments)))
      {:action (first arguments) :options options}
      :else
      {:exit-message (usage summary)})))

(defn- exit [status msg]
  (println msg)
  (System/exit status))

(defn run [args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (binding [*verbose* (:verbose options)]
        (command {:action action :args options})))))

(run *command-line-args*)

