(ns sap.apps
  (:require
    [clojure.string :as str]
    [sap.client :refer [apps executors]]
    [sap.utils :refer [fail now ->inst  duration]]))


(def wide-info
  (delay
    (let [add-executors (fn [executors {:keys [id] :as app}]
                          (let [pods (filter #(= id (:app %)) executors)]
                            (assoc app :executors (count pods))))
          add-duration  (fn [{:keys [age created-at terminated-at] :as app}]
                          (let [duration (if (some? terminated-at)
                                           (duration (->inst terminated-at) (->inst created-at))
                                           age)]
                            (assoc app :duration duration)))]
      (comp
        (map add-duration)
        (map (partial add-executors (executors)))))))


(defn- find-app
  ([partial-id]
   (find-app (apps) partial-id))
  ([apps partial-id]
   (if-let [app (some (fn [{:keys [id] :as app}] (and (str/includes? id partial-id) app)) apps)]
     app
     (fail (format "Unable to find application \"%s\"" partial-id)))))


(defn find-by
  [{:keys [state id days prefix wide]}]
  (let [apps (if (some? id)
               [(find-app (apps) id)]
               (apps state))
        older? (fn [{:keys [created-at]}]
                 (let [diff (.toDays (java.time.Duration/between (->inst created-at) (now)))]
                   (>= diff days)))
        apps (cond->> apps
                 (some? days) (filter older?)
                 (some? prefix) (filter (fn [{:keys [id]}]
                                          (str/starts-with? id prefix)))
                 (some? wide) (into  [] @wide-info))]
    apps))

