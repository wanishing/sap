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

(defn- parse-age [start end]
  (let [diff (java.time.Duration/between end start)
        units [[(.toDays diff) "d"]
               [(mod (.toHours diff) 24) "h"]
               [(mod (.toMinutes diff) 60) "m"]
               [(mod (.toSeconds diff) 60) "s"]]]
    (->> units
         (filter (fn [[diff _]] (pos? diff)))
         (map (fn [[diff unit]] (format "%d%s" diff unit)))
         (string/join))))

(defn- parse-app [[id created-at state]]
  (let [parse-age (partial parse-age (now))]
    {:id id
     :state state
     :created-at created-at
     :age (parse-age (to-inst created-at))}))

(defn- spark-apps
  ([state]
   (if-let [state (and (some? state) (string/upper-case state))]
     (filter (fn [app]
               (= (:state app) state)) (spark-apps))
     (spark-apps)))
  ([]
   (let [raw-apps (run-sh "kubectl" "get" "sparkapplication" "-o=jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.metadata.creationTimestamp}{\"\\t\"}{.status.applicationState.state}{\"\\n\"}{end}")
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

(defn- delete-app [resource id]
  (run-sh "kubectl" "delete" resource id))

(defn- reapply [resource id]
  (let [app (json/parse-string (run-sh "kubectl" "get" resource id "-o" "json") true)
        fresh-metadata (select-keys (:metadata app) [:name :namespace])
        fresh-app (-> app
                      (assoc :metadata fresh-metadata)
                      (dissoc :status)
                      (json/generate-string))
        fname (format "/tmp/%s.json" id)]
    (spit fname fresh-app)
    (spit (format "/tmp/debug-%s.json" id) app)
    (println (format "Fresh app created at %s" fname))
    (delete-app resource id)
    (println "Old app deleted")
    (run-proc "kubectl" "apply" "-f" fname)))

(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods"})

(def commands-map {:delete (fn [{:keys [id]}]
                             (delete-app "sparkapplication" id))

                   :apps spark-apps

                   :states #{"FAILED" "COMPLETED"}

                   :ui spark-ui

                   :get (fn [{:keys [id]}]
                          (print (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml")))

                   :desc (fn [{:keys [id]}]
                           (print (run-sh "kubectl" "describe" "sparkapplication" id)))

                   :reapply (fn [{:keys [id]}]
                              (reapply "sparkapplication" id))

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
                              (:app))]
                  {:executor pod :app app}))
        fields-selector "-o=jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.metadata.labels}{\"\\n\"}{end}"]
    (->> (run-sh "kubectl" "get" "pods" "-l" "spark-role=executor" fields-selector)
         (string/split-lines)
         (map parse))))

(defn- find-apps-by
  [{:keys [state id age prefix wide]}]
  (let [apps (command-factory :apps)
        apps  (if (some? id)
                [(find-app (apps) id)]
                (apps state))
        older? (fn [{:keys [created-at]}]
                 (let [diff (.toDays (java.time.Duration/between (to-inst created-at) (now)))]
                   (>= diff age)))
        attach-executors (fn [executors {:keys [id] :as app}]
                           (let [pods (filter #(= id (:app %)) executors)]
                             (assoc app :executors (count pods))))
        apps (cond->> apps
               (some? age) (filter older?)
               (some? prefix) (filter (fn [{:keys [id]}]
                                        (string/starts-with? id prefix)))
               (some? wide) (map (partial attach-executors (fetch-executors))))]
    apps))

(defn- command-runner [cmd options]
  (let [app (first (find-apps-by options))
        cmd (command-factory cmd)]
    (cmd app)))

(defmulti command
  (fn [{:keys [action]}] (keyword action)))

(defmethod command :delete [{:keys [args]}]
  (let [apps (find-apps-by args)
        cleaner (command-factory :delete)]
    (doseq [app apps]
      (cleaner app)
      (println "Deleted" (:id app)))
    (println "Done.")))

(defmethod command :cleanup [{:keys [args]}]
  (doseq [state (command-factory :states)]
    (println "Starting cleanup of" state "jobs")
    (command {:action :delete :args (assoc args :state state)})))

(defmethod command :ls [{:keys [args]}]
  (->> (find-apps-by args)
       (sort-by (juxt :id :created-at))
       (map #(dissoc % :created-at))
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
  (command-runner :reapply args))

(def cli-options
  [["-s" "--state STATE" "State of the apps to be deleted"]
   ["-i" "--id ID" "Appliaction id, can be supplied partially"]
   [nil "--age AGE" "Age of apps to be  deleted"
    :parse-fn #(Integer/parseInt %)
    :default 0]
   ["-p" "--prefix PREFIX" "Prefix of apps to be  deleted"]
   ["-w" "--wide"]
   ["-v" "--verbose"]
   ["-h" "--help"]])

(defn- usage [options-summary]
  (->> ["Common actions against k8s spark applications."
        ""
        "Usage: bb sap.clj action [options]"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  delete        delete appliactions by state, or by (possibly partial) id, or by age"
        "  cleanup       delete COMPLETED/FAILED/Failed applications, optionally by age"
        "  ls            list appliactions, optionally by state or age"
        "  ui            access application UI given job id"
        "  get           get yaml of applicatio given job id and mode"
        "  desc          alias for `kubectl describe` command"
        "  logs          alias for `kubectl logs` command"
        "  pods          display all the pods associated to the given application id"
        "  reapply       re-apply existing application by id (current application will be deleted)"
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

