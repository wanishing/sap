(ns sap.commands.logs
  (:require
    [sap.client :as client]))


(defn logs
  [{:keys [driver]} _]
  (client/logs driver))

