#!/usr/bin/env bb
(ns sap.core
  (:gen-class)
  (:require
    [babashka.process :refer [process]]
    [cheshire.core :refer [parse-string]]
    [clj-yaml.core :as yaml]
    [clojure.java.io :as io]
    [clojure.java.shell :refer [sh]]
    [clojure.string :as str]
    [clojure.tools.cli :refer [parse-opts]])
  (:import
    (java.net
      Socket
      SocketException)))


(def ^:dynamic *verbose* nil)


(defn- exit
  ([status]
   (System/exit status))
  ([status msg]
   (println msg)
   (exit status)))


(def fail (partial exit 1))


(defn- run-sh
  [& args]
  (when *verbose*
    (println (str/join " " args)))
  (let [{:keys [out exit err]} (apply sh args)]
    (if (zero? exit)
      out
      (fail (format
              "Failed to execute command:\n \"%s\"\nError:\n %s"
              (str/join " " args) err)))))


(defn- run-proc
  [& args]
  (when *verbose*
    (println (str/join " " args)))
  @(process args {:out :inherit})
  nil)


(defn- now
  []
  (java.time.Instant/now))


(defn- ->inst
  [s]
  (java.time.Instant/parse s))


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


(defn- duration
  [start end]
  (let [diff   (java.time.Duration/between end start)
        units  [[(.toDays diff) "d"]
                [(mod (.toHours diff) 24) "h"]
                [(mod (.toMinutes diff) 60) "m"]
                [(mod (.toSeconds diff) 60) "s"]]
        result (->> units
                    (filter (fn [[diff _]] (pos? diff)))
                    (map (fn [[diff unit]] (format "%d%s" diff unit)))
                    (str/join))]
    result))


(defn- parse-app
  [[id created-at state terminated-at]]
  {:id            id
   :state         (or state "UNKNOWN")
   :created-at    created-at
   :terminated-at (if (= terminated-at "<nil>") nil terminated-at)
   :driver   (format "%s-driver" id)
   :age      (duration (now) (->inst created-at))})


(defn- jsonpath
  [fields]
  (let [formatted-fields (->> fields
                              (map #(format "{%s}" %))
                              (str/join "{\"\\t\"}"))
        jsonpath         (format
                           "-o=jsonpath={range .items[*]}%s{\"\\n\"}{end}"
                           formatted-fields)]
    jsonpath))


(defn- spark-apps
  ([state]
   (if-let [given-state (and (some? state) (str/upper-case state))]
     (filter (fn [{:keys [state]}]
               (= state given-state)) (spark-apps))
     (spark-apps)))
  ([]
   (let [raw-apps (run-sh "kubectl" "get" "sparkapplication"
                          (jsonpath
                            [".metadata.name"
                             ".metadata.creationTimestamp"
                             ".status.applicationState.state"
                             ".status.terminationTime"]))
         apps     (->> raw-apps
                       (str/split-lines)
                       (filter #(not (str/blank? %)))
                       (map #(str/split % #"\t"))
                       (map parse-app))]
     apps)))


(defn- forward-port
  [driver port]
  (run-sh "kubectl" "port-forward" driver (format "%s:4040" port)))


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
       (>= port limit)
         (fail "Unable to find available port")
       (not (busy? port))
         port
       :else
         (recur (inc port))))))


(defn- spark-ui
  [{:keys [driver]} _]
  (let [port  (find-free-port)]
    (println "Port forwarding" driver
             (format "to http://localhost:%s..." port))
    (forward-port driver port)))


(declare command-factory)


(defn- find-app
  ([partial-id]
   (find-app ((command-factory :apps)) partial-id))
  ([apps partial-id]
   (if-let [app (some (fn [{:keys [id] :as app}] (and (str/includes? id partial-id) app)) apps)]
     app
     (fail (format "Unable to find application \"%s\"" partial-id)))))


(defn- fresh-app
  [raw-app]
  (let [app            (yaml/parse-string raw-app)
        fresh-metadata (select-keys (:metadata app) [:name :namespace])
        fresh-app      (-> app
                           (assoc :metadata fresh-metadata)
                           (dissoc :status))]
    fresh-app))


(defn- yaml
  [id]
  (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml"))


(defn- delete
  ([{:keys [id]} _]
   (delete id))
  ([id]
   (run-proc "kubectl" "delete" "sparkapplication" id)))


(defn- describe
  [{:keys [id]} _]
  (run-proc "kubectl" "describe" "sparkapplication" id))


(defn- logs
  [{:keys [driver]} _]
  (run-proc "kubectl" "logs" "-f" driver))


(defn- pods
  [{:keys [id]} _]
  (let [label (format "sparkoperator.k8s.io/app-name=%s" id)]
    (run-proc "kubectl" "get" "pods" "-l" label)))


(defn- reapply
  [{:keys [id]} {:keys [image]}]
  (let [raw-app   (yaml id)
        fresh-app (cond-> (fresh-app raw-app)
                      (some? image) (assoc-in [:spec :image] image))
        fresh-app (yaml/generate-string fresh-app)
        fname     (format "/tmp/%s.yaml" id)]
    (spit fname fresh-app)
    (println (format "Fresh app created at %s" fname))
    (delete id)
    (run-proc "kubectl" "apply" "-f" fname)))


(defn- get-yaml
  [{:keys [id]} {:keys [fresh]}]
  (let [yaml (cond-> (yaml id)
                 (some? fresh) (fresh-app))]
    (println (yaml/generate-string yaml))))


(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods"})


(def command-by-name
  {:delete delete

   :apps spark-apps

   :ui spark-ui

   :get get-yaml

   :desc describe

   :reapply reapply

   :logs logs

   :pods pods})


(defn- command-factory
  [cmd]
  (get-in command-by-name [cmd]))


(defn- fetch-executors
  []
  (let [parse (fn [extr]
                (let [[pod, labels] (str/split extr #"\t")
                      app (-> labels
                              (parse-string true)
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
          add-duration  (fn [{:keys [age created-at terminated-at] :as app}]
                          (let [duration (if (some? terminated-at)
                                           (duration (->inst terminated-at) (->inst created-at))
                                           age)]
                            (assoc app :duration duration)))]
      (comp
        (map add-duration)
        (map (partial add-executors (fetch-executors)))))))


(defn- find-apps-by
  [{:keys [state id days prefix wide]}]
  (let [apps (command-factory :apps)
        apps (if (some? id)
               [(find-app (apps) id)]
               (apps state))
        older? (fn [{:keys [created-at]}]
                 (let [diff (.toDays (java.time.Duration/between (->inst created-at) (now)))]
                   (>= diff days)))
        apps (cond->> apps
                 (some? days) (filter older?)
                 (some? prefix) (filter (fn [{:keys [id]}]
                                          (str/starts-with? id prefix)))
                 (some? wide) (into  [] @wide-info))]
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
   [nil "--version"]
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
        ""]
       (str/join \newline)))


(defn- error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))


(defn- version
  []
  (slurp (io/file "./resources/VERSION")))


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
      (:version options)
        {:exit-message (version) :ok? true}
      (:help options)
        {:exit-message (usage summary) :ok? true}
      errors
        {:exit-message (error-msg errors)}
      (and (= 1 (count arguments))
           cmd)
        {:action (validate-command commands cmd) :options options}
      :else
        {:exit-message (usage summary)})))


(defn -main
  [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (do
        (binding [*verbose* (:verbose options)]
          (command {:action action :args options}))
        (exit 0)))))

