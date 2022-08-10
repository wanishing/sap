(ns sap.commands.yaml
  (:require
    [clj-yaml.core :as yaml]
    [sap.client :refer [fetch-yaml fetch-fresh-yaml]]))


(defn yaml
  [{:keys [id]} {:keys [fresh]}]
  (let [yml (if (some? fresh)
              (fetch-fresh-yaml id)
              (fetch-yaml id))]
    (println (yaml/generate-string yml))))

