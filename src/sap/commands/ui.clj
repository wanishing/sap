(ns sap.commands.ui
  (:require
    [sap.client :refer [forward-port]]
    [sap.utils :refer [fail]])
  (:import
    (java.net
      Socket
      SocketException)))


(defn- busy?
  [port]
  (try
    (nil? (.close (Socket. "localhost" port)))
    (catch SocketException _
      false)))


(defn- find-free-port
  ([]
   (find-free-port 4040 (+ 50 4040)))
  ([start limit]
   (loop [port start]
     (cond
       (>= port limit)
         (fail "Unable to find available port")
       (not (busy? port))
         port
       :else
         (recur (inc port))))))


(defn ui
  [{:keys [driver]} _]
  (let [port  (find-free-port)]
    (println "Port forwarding" driver
             (format "to http://localhost:%s..." port))
    (forward-port driver port)))

