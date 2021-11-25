#!/usr/bin/env bb
(ns sap.core
  (:require
   [babashka.curl :as curl]
   [babashka.wait :as wait]
   [sap.client :as client]
   [sap.utils :as utils]
   [cheshire.core :as json]
   [clj-yaml.core :as yaml]
   [clojure.core.async :as async]
   [clojure.pprint :refer [cl-format]]
   [clojure.string :as str]
   [clojure.tools.cli :refer [parse-opts]])
  (:import
   (java.net
    Socket
    SocketException)))

(def ^:dynamic *verbose* nil)

(def fail (partial utils/exit 1))


(defn- print-rows
  [rows]
  (when (seq rows)
    (let [divider (apply str (repeat 4 " "))
          cols    (keys (first rows))
          headers (map #(str/upper-case (name %)) cols)
          widths  (map
                    (fn [k]
                      (apply max (count (str k)) (map #(count (str (get % k))) rows)))
                    cols)
          fmts    (map #(str "%-" % "s") widths)
          fmt-row (fn [row]
                    (apply str
                           (interpose divider
                                      (for [[col fmt]
                                                                                                                                                         (map vector (map #(get row %) cols) fmts)]
                                                                                                                                                     (format fmt (str col))))))]
      (println (fmt-row (zipmap cols headers)))
      (doseq [row rows]
        (println (fmt-row row))))))

(defn- busy?
  [port]
  (try
    (nil? (.close (Socket. "localhost" port)))
    (catch SocketException _
      false)))


(defn- find-free-port
  ([]
   (find-free-port 4040 (+ 50 4040)))
  ([start limit]
   (loop [port start]
     (cond
       (>= port limit) (fail "Unable to find available port")
       (not (busy? port)) port
       :else
       (recur (inc port))))))


(defn- spark-ui
  [{:keys [driver]} _]
  (let [port  (find-free-port)]
    (println "Port forwarding" driver
             (format "to http://localhost:%s..." port))
    (client/forward-port driver port)))


(defn- localhost
  [port]
  (format "localhost:%s" port))


(defn- get-by
  [api]
  (let [headers     {"Accept" "application/json"}
        resp        (curl/get api {:headers headers
                                   :throw   false})
        ->snakecase (fn [s]
                      (->> s
                           (partition-by #(java.lang.Character/isUpperCase %))
                           (map (fn [subs]
                                  (if (= (count subs) 1)
                                    (str "-" (str/lower-case (first subs)))
                                    (apply str subs))))
                           (str/join)))
        body        (when (= (:status resp) 200)
                      (-> resp
                          :body
                          (json/parse-string (comp keyword ->snakecase))))]
    body))


(defn- ->millis
  [millis]
  (if (or (nil? millis) (zero? millis))
    "0.0ms"
    (let [inst  (utils/at-utc (java.time.Instant/ofEpochMilli millis))
          units [[(.getHour inst) "h"]
                 [(.getMinute inst) "m"]
                 [(.getSecond inst) "s"]
                 [(int (mod (Math/round millis) 1000)) "ms"]]
          res   (->> units
                     (filter (fn [[val _]] (pos? val)))
                     (map (fn [[val unit]] (format "%d%s" val unit)))
                     (str/join))]
      res)))


(defn- ->bytes
  [bytes]
  (if (some? bytes)
    (let [units      [[(double (/ bytes (* 1024 1024 1024))) "GiB"]
                      [(double (/ bytes (* 1024 1024))) "MiB"]
                      [(double (/ bytes 1024)) "KiB"]
                      [bytes "B"]]
          [val unit] (some (fn [[val unit]]
                             (and (>= val 1) [val unit])) units)
          res        (format "%.1f%s" val unit)]
      res)
    0.0))


(defn- fetch-app
  [endpoint]
  (let [api          (str endpoint "/api/v1/applications")
        {:keys [id]} (first (get-by api))]
    id))


(defn- bytes-per-records
  [bytes records]
  (when (every? pos? [bytes records])
    (format "%s/%s" (->bytes bytes) (cl-format nil "~:d" (long records)))))


(defn- fetch-metrics
  [endpoint app-id stage-id attempt-id]
  (let [percentiles [0.01 0.25 0.5 0.75 0.99]
        percentiles-names ["Min" "25th" "Median" "75th" "Max"]
        api (str endpoint (format "/api/v1/applications/%s/stages/%s/%s/taskSummary?quantiles=%s"
                                  app-id
                                  stage-id
                                  attempt-id
                                  (str/join "," percentiles)))
        {:keys                                   [executor-run-time jvm-gc-time] :as response
         {:keys [bytes-read records-read]}       :input-metrics
         {:keys [bytes-written records-written]} :output-metrics
         {:keys [read-bytes read-records]}       :shuffle-read-metrics
         {:keys [write-bytes write-records]}     :shuffle-write-metrics} (get-by api)
        parse-percentile (fn [idx quantile]
                           (let [nth #(nth % idx)]
                             {:percentile    quantile
                              :duration      (->millis (nth executor-run-time))
                              :gc-time       (->millis (nth jvm-gc-time))
                              :input         (bytes-per-records (nth bytes-read) (nth records-read))
                              :output        (bytes-per-records (nth bytes-written) (nth records-written))
                              :shuffle-read  (bytes-per-records (nth read-bytes) (nth read-records))
                              :shuffle-write (bytes-per-records (nth write-bytes) (nth write-records))}))
        metrics (if (some? response)
                  (map-indexed parse-percentile
                               percentiles-names)
                  [])
        nullable-fields (filter (fn [field]
                                  (every? nil? (map field metrics)))
                                [:input :output :shuffle-read :shuffle-write])
        metrics (map (fn [m]
                       (apply (partial dissoc m) nullable-fields)) metrics)]
    metrics))


(defn- parse-stage
  [{:keys [stage-id
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
           name]}]
  (let [stage {:stage-id      stage-id
               :attempt-id    attempt-id
               :duration      (let [sbt (java.time.ZonedDateTime/parse submission-time
                                                                       (java.time.format.DateTimeFormatter/ofPattern
                                                                         "yyyy-MM-dd'T'HH:mm:ss.SSSz"))
                                    sbt (utils/at-utc (.toInstant sbt))
                                    now (utils/at-utc (utils/now))]
                                (utils/duration now sbt))
               :description   (or (and description (first (str/split-lines description))) name)
               :input         (bytes-per-records input-bytes input-records)
               :output        (bytes-per-records output-bytes output-records)
               :shuffle-write (bytes-per-records shuffle-write-bytes shuffle-write-records)
               :shuffle-read  (bytes-per-records shuffle-read-bytes shuffle-read-records)
               :tasks         {:succeeded num-complete-tasks
                               :failed    num-failed-tasks
                               :active (if (neg? num-active-tasks)
                                         (- num-active-tasks)
                                         num-active-tasks)
                               :total     num-tasks}}
        nullable-fields (filter #(nil? (% stage))
                                [:input :output :shuffle-read :shuffle-write])
        stage           (apply (partial dissoc stage) nullable-fields)]
    stage))


(defn- fetch-stages
  [endpoint id]
  (let [api (str endpoint (format "/api/v1/applications/%s/stages?status=active" id))
        stages     (get-by api)]
    (when (seq stages)
      (->> stages
           (map parse-stage)
           (filter #(pos? (get-in % [:tasks :active])))
           (sort-by :stage-id)))))


(defn- fetch-executors-count
  [endpoint id]
  (let [api (str endpoint (format "/api/v1/applications/%s/executors" id))
        pods (get-by api)]
    (if (seq pods)
      (dec (count pods))
      0)))


(defn- print-metrics
  [{:keys [duration
           stage-id
           attempt-id
           description
           input
           output
           shuffle-write
           shuffle-read]
    {:keys [total active failed succeeded]} :tasks} metrics]
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


(defn- clear
  []
  (println "\033[H\033[2J"))


(declare command-factory)


(defn- find-app
  ([partial-id]
   (find-app ((command-factory :apps)) partial-id))
  ([apps partial-id]
   (if-let [app (some (fn [{:keys [id] :as app}] (and (str/includes? id partial-id) app)) apps)]
     app
     (fail (format "Unable to find application \"%s\"" partial-id)))))


(defn- running?
  ([id]
   (let [{:keys [state]} (find-app id)]
     (= state "RUNNING")))
  ([id fetch-stage]
   (let [running? (running? id)
         active?  #(let [start (.minusSeconds (utils/now) 1)]
                     (loop []
                       (if (some? (fetch-stage)) true
                           (do
                             (clear)
                             (print (format "\rWaiting for active stage for %s" (utils/duration (utils/now) start)))
                             (doseq [c (repeat 5 ".")]
                               (print c)
                               (flush)
                               (Thread/sleep 1000))
                             (recur)))))]
     (and running? (active?)))))


(defn- metrics
  [{:keys [driver id]} _]
  (when (running? id)
    (let [port       (find-free-port)]
      (async/thread (client/forward-port driver port))
      (wait/wait-for-port "localhost" port)
      (let [endpoint      (localhost port)
            app-id        (fetch-app endpoint)
            fetch-executors (partial fetch-executors-count endpoint app-id)
            fetch-stages   (partial fetch-stages endpoint app-id)
            fetch-metrics (partial fetch-metrics endpoint app-id)]
        (clear)
        (while (running? id fetch-stages)
          (let [stages (fetch-stages)
                stages (map (fn [{:keys [stage-id
                                         attempt-id] :as stage}]
                              {:metrics (fetch-metrics stage-id attempt-id)
                               :stage stage}) stages)]
            (println (with-out-str
                       (clear)
                       (println "Name:" id)
                       (println (format "UI: http://%s" (localhost port)))
                       (println "Application Id:" app-id)
                       (println "Executors Count:" (fetch-executors))
                       (doseq [{:keys [stage
                                       metrics]} stages]
                         (when (some? stage)
                           (println)
                           (print-metrics stage metrics)))))))))
    (println))
  (println (format "Application %s has no active stage" id)))


(defn- fresh-app
  [raw-app]
  (let [app            (yaml/parse-string raw-app)
        fresh-metadata (select-keys (:metadata app) [:name :namespace])
        fresh-app      (-> app
                           (assoc :metadata fresh-metadata)
                           (dissoc :status))]
    fresh-app))

(defn- delete
  ([{:keys [id]} _]
   (delete id))
  ([id]
   (client/delete id)))


(defn- describe
  [{:keys [id]} _]
  (client/describe id))


(defn- logs
  [{:keys [driver]} _]
  (client/logs driver))


(defn- pods
  [{:keys [id]} _]
  (client/pods id))


(defn- reapply
  [{:keys [id]} {:keys [image]}]
  (let [raw-app   (client/yaml id)
        fresh-app (cond-> (fresh-app raw-app)
                    (some? image) (assoc-in [:spec :image] image))
        fresh-app (yaml/generate-string fresh-app)
        fname     (format "/tmp/%s.yaml" id)]
    (spit fname fresh-app)
    (println (format "Fresh app created at %s" fname))
    (delete id)
    (client/apply-app fname)))


(defn- get-yaml
  [{:keys [id]} {:keys [fresh]}]
  (let [yaml (cond-> (client/yaml id)
               (some? fresh) (fresh-app))]
    (println (yaml/generate-string yaml))))


(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods" "metrics"})


(def command-by-name
  {:delete delete

   :apps client/apps

   :ui spark-ui

   :get get-yaml

   :desc describe

   :reapply reapply

   :logs logs

   :pods pods

   :metrics metrics})


(defn- command-factory
  [cmd]
  (get-in command-by-name [cmd]))


(def wide-info
  (delay
    (let [add-executors (fn [executors {:keys [id] :as app}]
                          (let [pods (filter #(= id (:app %)) executors)]
                            (assoc app :executors (count pods))))
          add-duration  (fn [{:keys [age created-at terminated-at] :as app}]
                          (let [duration (if (some? terminated-at)
                                           (utils/duration (utils/->inst terminated-at) (utils/->inst created-at))
                                           age)]
                            (assoc app :duration duration)))]
      (comp
        (map add-duration)
        (map (partial add-executors (client/fetch-executors)))))))


(defn- find-apps-by
  [{:keys [state id days prefix wide]}]
  (let [apps (command-factory :apps)
        apps (if (some? id)
               [(find-app (apps) id)]
               (apps state))
        older? (fn [{:keys [created-at]}]
                 (let [diff (.toDays (java.time.Duration/between (utils/->inst created-at) (utils/now)))]
                   (>= diff days)))
        apps (cond->> apps
               (some? days)   (filter older?)
               (some? prefix) (filter (fn [{:keys [id]}]
                                        (str/starts-with? id prefix)))
               (some? wide)   (into  [] @wide-info))]
    apps))


(defn- run-one
  [cmd options]
  (let [[app & _] (find-apps-by options)
        cmd (command-factory cmd)]
    (cmd app options)))


(defn- run-many
  [cmd options]
  (let [apps (find-apps-by options)
        cmd  (command-factory cmd)
        cmds (map #(future (cmd % options)) apps)]
    (doseq [cmd cmds]
      @cmd)))


(defmulti command
  (fn [{:keys [action]}] (keyword action)))


(defmethod command :delete [{:keys [args]}]
  (run-many :delete args))


(defmethod command :cleanup [{:keys [args]}]
  (doseq [state #{"FAILED" "COMPLETED"}]
    (println "Starting cleanup of" state "jobs")
    (command {:action :delete :args (assoc args :state state)})))


(defmethod command :ls [{:keys [args]}]
  (let [invisble-fields [:created-at :terminated-at :driver]]
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


(defn- usage
  [options-summary]
  (->> ["kubectl wrapper to perform common actions against Spark applications."
        ""
        "Usage: sap action [options]"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  delete        delete application"
        "  cleanup       delete COMPLETED/FAILED applications"
        "  ls            list applications"
        "  ui            port-forwarding application ui given id"
        "  get           alias for `kubectl get -o yaml` command"
        "  desc          alias for `kubectl describe` command"
        "  logs          alias for `kubectl logs` command"
        "  pods          display all pods associated to application"
        "  reapply       re-apply application (keeping the same id)"
        "  metrics       display metrics of the current active stage(s)"
        ""]
       (str/join \newline)))


(defn- error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))


(defn- validate-command
  [commands input]
  (let [cmds (filter #(and (str/starts-with? % input) %) commands)
        found (count cmds)]
    (cond
      (> found 1)
      (fail (format "Given command \"%s\" is ambiguous.\nFound: %s" input (str/join ", " cmds)))
      (zero? found)
      (fail (format "Unknown command \"%s\". \nRun --help for available commands" input))
      (= 1 found)
      (first cmds))))


(defn- validate-args
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        cmd (first arguments)]
    (cond
      (:help options)
      {:exit-message (usage summary) :ok? true}
      errors
      {:exit-message (error-msg errors)}
      (and (= 1 (count arguments))
           cmd)
      {:action (validate-command commands cmd) :options options}
      :else
      {:exit-message (usage summary)})))


(defn run
  [args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (utils/exit (if ok? 0 1) exit-message)
      (binding [*verbose* (:verbose options)]
        (command {:action action :args options})))))


(run *command-line-args*)


