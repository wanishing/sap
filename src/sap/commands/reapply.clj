(ns sap.commands.reapply
  (:require
    [clj-yaml.core :as yaml]
    [sap.client :refer [fetch-fresh-yaml create delete]]))


(defn reapply
  [{:keys [id]} {:keys [image]}]
  (let [fresh-app (cond-> (fetch-fresh-yaml id)
                      (some? image) (assoc-in [:spec :image] image))
        fresh-app (yaml/generate-string fresh-app)
        fname     (format "/tmp/%s.yaml" id)]
    (spit fname fresh-app)
    (println (format "Fresh app created at %s" fname))
    (delete id)
    (create fname)))

