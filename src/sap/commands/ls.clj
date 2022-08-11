(ns sap.commands.ls
  (:require
    [sap.apps :as apps]
    [sap.utils :refer [print-rows]]))


(defn ls
  [_ args]
  (let [invisble-fields [:created-at :terminated-at :driver]]
    (->> (apps/find-by args)
         (sort-by (juxt :id :created-at))
         (map #(reduce (fn [app key] (dissoc app key)) % invisble-fields))
         (print-rows))))
