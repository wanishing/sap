(ns sap.commands.delete
  (:require
    [sap.client :as client]))


(defn delete
  ([{:keys [id]} _]
   (client/delete id)))

