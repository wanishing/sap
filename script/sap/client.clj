(ns sap.client
  (:require
   [babashka.curl :as curl]
   [babashka.process :refer [process]]
   [sap.utils :as utils]
   [cheshire.core :as json]
   [clj-yaml.core :as yaml]
   [clojure.java.shell :refer [sh]]
   [clojure.string :as str])
  (:import
   java.util.Base64))

(def ^:dynamic *verbose* nil)


(def fail (partial utils/exit 1))

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


(defn- parse-app
  [[id created-at state terminated-at]]
  {:id            id
   :state         (or state "UNKNOWN")
   :created-at    created-at
   :terminated-at (if (= terminated-at "<nil>") nil terminated-at)
   :driver   (format "%s-driver" id)
   :age      (utils/duration created-at)})

(defn- jsonpath
  [fields]
  (let [formatted-fields (->> fields
                              (map #(format "{%s}" %))
                              (str/join "{\"\\t\"}"))
        jsonpath         (format
                          "-o=jsonpath={range .items[*]}%s{\"\\n\"}{end}"
                          formatted-fields)]
    jsonpath))

(defn forward-port
  [driver port]
  (run-sh "kubectl" "port-forward" driver (format "%s:4040" port)))


(defn logs
  [driver]
  (run-proc "kubectl" "logs" "-f" driver))

(defn describe
  [id]
  (run-proc "kubectl" "describe" "sparkapplication" id))

(defn delete [id]
  (run-proc "kubectl" "delete" "sparkapplication" id))

(defn yaml
  [id]
  (run-sh "kubectl" "get" "sparkapplication" id "-o" "yaml"))

(defn pods
  [id]
  (let [label (format "sparkoperator.k8s.io/app-name=%s" id)]
    (run-proc "kubectl" "get" "pods" "-l" label)))

(defn apply-app
  [file]
  (run-proc "kubectl" "apply" "-f" file))


(defn apps
  ([state]
   (if-let [given-state (and (some? state) (str/upper-case state))]
     (filter (fn [{:keys [state]}]
               (= state given-state)) (apps))
     (apps)))
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

(defn fetch-executors
  []
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

(comment
  
(def k8s-config (let [path (str (System/getProperty "user.home") "/.kube/config")
                      ctx (-> path
                               slurp
                               (yaml/parse-string))]
                  ctx))

(def api-server (str (run-sh "kubectl" "config" "view" "--minify" "-o" "jsonpath={.clusters[0].cluster.server}") "/api"))

(def secret (run-sh "kubectl" "get" "serviceaccount" "default" "-o" "jsonpath={.secrets[0].name}"))

(def token (run-sh "kubectl" "get" "secret" secret "-o" "jsonpath={.data.token}"))

(def config-token (-> k8s-config
                       :users
                       first
                       (get-in [:user :auth-provider :config :id-token])))

(defn decode [to-decode]
  (String. (.decode (Base64/getDecoder) to-decode)))

(curl/get api-server {:headers {"Authorization" (format "Bearer %s" (decode token))}
                      :raw-args ["--insecure"]})

(curl/get api-server {:headers {"Authorization" (format "Bearer %s" config-token)}
                      :raw-args ["--insecure"]})
)
