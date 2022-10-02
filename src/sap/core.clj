#!/usr/bin/env bb
(ns sap.core
  (:gen-class)
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.cli :refer [parse-opts]]
    [sap.apps :as apps]
    [sap.commands.delete :refer [delete]]
    [sap.commands.describe :refer [describe]]
    [sap.commands.logs :refer [logs]]
    [sap.commands.ls :refer [ls]]
    [sap.commands.pods :refer [pods]]
    [sap.commands.reapply :refer [reapply]]
    [sap.commands.ui :refer [ui]]
    [sap.commands.yaml :refer [yaml]]
    [sap.utils :as utils :refer [fail exit]]))


(declare command-factory)


(def commands #{"delete" "cleanup" "ls" "ui" "get" "desc" "logs" "reapply" "pods"})


(def command-by-name
  {:delete delete

   :ls ls

   :ui ui

   :get yaml

   :desc describe

   :reapply reapply

   :logs logs

   :pods pods})


(defn- command-factory
  [cmd]
  (get-in command-by-name [cmd]))


(defn- run-one
  [cmd options]
  (let [[app & _] (apps/find-by options)
        cmd (command-factory cmd)]
    (cmd app options)))


(defn- run-many
  [cmd options]
  (let [apps (apps/find-by options)
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
  (run-one :ls args))


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
  (slurp (io/resource "VERSION")))


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
        (binding [utils/*verbose* (:verbose options)]
          (command {:action action :args options}))
        (exit 0)))))

