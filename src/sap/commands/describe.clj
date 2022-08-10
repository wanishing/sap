(ns sap.commands.describe
  (:require
    [sap.client :as client]))


(defn describe
  ([{:keys [id]} _]
   (client/describe id)))

