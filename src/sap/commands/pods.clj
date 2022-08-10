(ns sap.commands.pods
  (:require
    [sap.client :as client]))


(defn pods
  [{:keys [id]} _]
  (let [label (format "sparkoperator.k8s.io/app-name=%s" id)]
    (client/pods label)))

