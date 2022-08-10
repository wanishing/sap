(ns sap.client
  (:gen-class)
  (:require
    [cheshire.core :refer [parse-string]]
    [clj-yaml.core :as yaml]
    [clojure.string :as str]
    [sap.utils :refer [now ->inst duration run-proc run-sh]]))


(defn- jsonpath
  [fields]
  (let [formatted-fields (->> fields
                              (map #(format "{%s}" %))
                              (str/join "{\"\\t\"}"))
        jsonpath         (format
                           "-o=jsonpath={range .items[*]}%s{\"\\n\"}{end}"
                           formatted-fields)]
    jsonpath))


(defn- fresh-app
  [raw-app]
  (let [app            (yaml/parse-string raw-app)
        fresh-metadata (select-keys (:metadata app) [:name :namespace :labels])
        fresh-app      (-> app
                           (assoc :metadata fresh-metadata)
                           (dissoc :status))]
    fresh-app))


(defn apps
  ([state]
   (if-let [given-state (and (some? state) (str/upper-case state))]
     (filter (fn [{:keys [state]}]
               (= state given-state)) (apps))
     (apps)))
  ([]
   (let [parse-app (fn [[id created-at state terminated-at]]
                     {:id            id
                      :state         (or state "UNKNOWN")
                      :created-at    created-at
                      :terminated-at (if (= terminated-at "<nil>") nil terminated-at)
                      :driver   (format "%s-driver" id)
                      :age      (duration (now) (->inst created-at))})
         raw-apps (run-sh "kubectl" "get" "sparkapplication"
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


(defn executors
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


(defn forward-port
  [driver port]
  (run-sh "kubectl" "port-forward" driver (format "%s:4040" port)))


(defn fetch-yaml
  [id]
  (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml"))


(defn fetch-fresh-yaml
  [id]
  (-> id
      fetch-yaml
      fresh-app))


(defn delete
  [id]
  (run-proc "kubectl" "delete" "sparkapplication" id))


(defn describe
  [id]
  (run-proc "kubectl" "describe" "sparkapplication" id))


(defn logs
  [driver]
  (run-proc "kubectl" "logs" "-f" driver))


(defn pods
  [label]
  (run-proc "kubectl" "get" "pods" "-l" label))


(defn create
  [fname]
  (run-proc "kubectl" "apply" "-f" fname))

