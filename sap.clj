#!/usr/bin/env bb
(ns sap
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as string]
            [cheshire.core :as json]
            [babashka.process :refer [process]]
            [clojure.tools.cli :refer [parse-opts]]))

(def ^:dynamic *verbose* nil)

(defn- run-sh [& args]
  (when *verbose*
    (println (string/join " " args)))
  (let [{:keys [out exit err]} (apply sh args)]
    (if (zero? exit)
      out
      (throw (ex-info err {:command args :error err})))))

(defn- run-proc [& args]
  (when *verbose*
    (println (string/join " " args)))
  @(process args {:out :inherit})
  nil)

(defn- print-apps [rows]
  (when (seq rows)
    (let [divider (apply str (repeat 4 " "))
          cols (keys (first rows))
          headers (map #(string/upper-case (name %)) cols)
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

(defn- spark-driver-app [id]
  (format "%s-driver" id))

(defn- now [] (java.time.Instant/now))

(defn- to-inst [s] (java.time.Instant/parse s))

(defn- parse-duration [start end]
  (let [diff (java.time.Duration/between end start)
        units [[(.toDays diff) "d"]
               [(mod (.toHours diff) 24) "h"]
               [(mod (.toMinutes diff) 60) "m"]
               [(mod (.toSeconds diff) 60) "s"]]]
    (->> units
         (filter (fn [[diff _]] (pos? diff)))
         (map (fn [[diff unit]] (format "%d%s" diff unit)))
         (string/join))))

(defn- parse-app [[id created-at state terminated-at]]
  (let [parse-duration (partial parse-duration (now))]
    {:id id
     :state state
     :created-at created-at
     :terminated-at (if (= terminated-at "<nil>") nil terminated-at)
     :age (parse-duration (to-inst created-at))}))

(defn- format-jsonpath [fields]
  (let [formatted-fields (->> fields
                              (map #(format "{%s}" %))
                              (string/join "{\"\\t\"}"))
        jsonpath (format "-o=jsonpath={range .items[*]}%s{\"\\n\"}{end}" formatted-fields)]
    jsonpath))

(defn- spark-apps
  ([state]
   (if-let [state (and (some? state) (string/upper-case state))]
     (filter (fn [app]
               (= (:state app) state)) (spark-apps))
     (spark-apps)))
  ([]
   (let [raw-apps (run-sh "kubectl" "get" "sparkapplication" (format-jsonpath
                                                              [".metadata.name"
                                                               ".metadata.creationTimestamp"
                                                               ".status.applicationState.state"
                                                               ".status.terminationTime"]))
         apps (->> raw-apps
                   (string/split-lines)
                   (filter #(not (string/blank? %)))
                   (map #(string/split % #"\t"))
                   (map parse-app))]
     apps)))

(defn- spark-ui [{:keys [id]}]
  (let [driver-app (spark-driver-app id)
        start 4040
        end (+ start 10)
        forward-port (fn [port]
                       (println "Port forwarding" driver-app (format "to http://localhost:%s..." port))
                       (run-sh "kubectl" "port-forward" driver-app (format "%s:4040" port)))
        busy? (fn [port]
                (= (:exit (sh "lsof" "-i" (format ":%d" port))) 0))]
    (loop [port start]
      (when (< port end)
        (if (not (busy? port))
          (forward-port port)
          (recur (inc port)))))))

(declare command-factory)

(defn- reapply [id]
  (let [app (json/parse-string (run-sh "kubectl" "get" "sparkapplication" id "-o" "json") true)
        fresh-metadata (select-keys (:metadata app) [:name :namespace])
        fresh-app (-> app
                      (assoc :metadata fresh-metadata)
                      (dissoc :status)
                      (json/generate-string))
        fname (format "/tmp/%s.json" id)
        delete (command-factory :delete)]
    (spit fname fresh-app)
    (spit (format "/tmp/debug-%s.json" id) app)
    (println (format "Fresh app created at %s" fname))
    (delete {:id id})
    (println "Old app deleted")
    (run-proc "kubectl" "apply" "-f" fname)))

(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods"})

(def commands-map {:delete (fn [{:keys [id]}]
                             (run-proc "kubectl" "delete" "sparkapplication" id))

                   :apps spark-apps

                   :states #{"FAILED" "COMPLETED"}

                   :ui spark-ui

                   :get (fn [{:keys [id]}]
                          (print (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml")))

                   :desc (fn [{:keys [id]}]
                           (print (run-sh "kubectl" "describe" "sparkapplication" id)))

                   :reapply (fn [{:keys [id]}]
                              (reapply id))

                   :logs (fn [{:keys [id]}]
                           (run-proc "kubectl" "logs" "-f" (spark-driver-app id)))

                   :pods (fn [{:keys [id]}]
                           (let [label (format "sparkoperator.k8s.io/app-name=%s" id)]
                             (run-proc "kubectl" "get" "pods" "-l" label)))})

(defn- command-factory [cmd]
  (get-in commands-map [cmd]))

(defn- find-app [apps partial-id]
  (if-let [app (some (fn [{:keys [id] :as app}] (and (string/includes? id partial-id) app)) apps)]
    app
    (throw (ex-info "Failed to find app" {:id partial-id}))))

(defn- fetch-executors []
  (let [parse (fn [extr]
                (let [[pod, labels] (string/split extr #"\t")
                      app (-> labels
                              (json/parse-string true)
                              (:sparkoperator.k8s.io/app-name))]
                  {:executor pod :app app}))
        executors (->> (run-sh "kubectl" "get" "pods" "-l" "spark-role=executor" (format-jsonpath
                                                                                  [".metadata.name"
                                                                                   ".metadata.labels"]))
                       (string/split-lines)
                       (map parse))]
    executors))

(def wide-info
  (let [add-executors (fn [executors {:keys [id] :as app}]
                        (let [pods (filter #(= id (:app %)) executors)]
                          (assoc app :executors (count pods))))
        add-duration (fn [{:keys [age created-at terminated-at] :as app}]
                       (let [duration (if (some? terminated-at)
                                        (parse-duration (to-inst terminated-at) (to-inst created-at))
                                        age)]
                         (assoc app :duration duration)))]
    (comp
     (map add-duration)
     (map (partial add-executors (fetch-executors))))))

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
                                        (string/starts-with? id prefix)))
               (some? wide) (into [] wide-info))]
    apps))

(defn- command-runner [cmd options]
  (let [app (first (find-apps-by options))
        cmd (command-factory cmd)]
    (cmd app)))

(defn- commands-runner [cmd options]
  (let [apps (find-apps-by options)
        f (command-factory cmd)]
    (doseq [app apps]
      (f app))
    (println "Done.")))

(defmulti command
  (fn [{:keys [action]}] (keyword action)))

(defmethod command :delete [{:keys [args]}]
  (commands-runner :delete args))

(defmethod command :cleanup [{:keys [args]}]
  (doseq [state (command-factory :states)]
    (println "Starting cleanup of" state "jobs")
    (command {:action :delete :args (assoc args :state state)})))

(defmethod command :ls [{:keys [args]}]
  (->> (find-apps-by args)
       (sort-by (juxt :id :created-at))
       (map #(dissoc % :created-at :terminated-at))
       (print-apps)))

(defmethod command :pods [{:keys [args]}]
  (command-runner :pods args))

(defmethod command :ui [{:keys [args]}]
  (command-runner :ui args))

(defmethod command :get [{:keys [args]}]
  (command-runner :get args))

(defmethod command :desc [{:keys [args]}]
  (command-runner :desc args))

(defmethod command :logs [{:keys [args]}]
  (command-runner :logs args))

(defmethod command :reapply [{:keys [args]}]
  (commands-runner :reapply args))

(def cli-options
  [["-s" "--state STATE" "State of application"]
   ["-i" "--id ID" "Application id, can be supplied partially"]
   [nil "--days DAYES" "Minimum amount of days the application is alive"
    :parse-fn #(Integer/parseInt %)
    :default 0]
   ["-p" "--prefix PREFIX" "Prefix of application id"]
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
        "  delete        delete appliactions"
        "  cleanup       delete COMPLETED/FAILED/Failed applications"
        "  ls            list appliactions"
        "  ui            port-forwarding application ui given id"
        "  get           alias for `kubectl get -o yaml` command"
        "  desc          alias for `kubectl describe` command"
        "  logs          alias for `kubectl logs` command"
        "  pods          display all pods associated to application"
        ""]
       (string/join \newline)))

(defn- error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

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

