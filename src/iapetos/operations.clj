(ns iapetos.operations
  (:import [io.prometheus.client
            Counter$Child
            Histogram$Child
            Histogram$Timer
            Gauge$Child
            Gauge$Timer
            Summary$Child
            Summary$Timer]
           [io.prometheus.metrics.core.datapoints DistributionDataPoint Timer TimerApi]
           [io.prometheus.metrics.core.metrics Counter$DataPoint Gauge$DataPoint Histogram$DataPoint Summary$DataPoint]
           [java.lang.reflect Field]))

; I dislike this, but it seemed to be the shortest way to get these values from a
; <class>$DataPoint, since that does not have any methods to collect this info.
;
; Another way to do this is by adding a different code path when doing registry/get
; for a read of a distribution metric, so that it doesn't return a DataPoint. However,
; then it needs to run (.collect) and find the right snapshot that matches the label map.
;
; Opened https://github.com/prometheus/client_java/issues/1610 to check for alternatives.
(defn- get-distribution-values [^Class klass ^DistributionDataPoint datapoint]
  (letfn [(get-private-val [field-name]
            (when-let [f ^Field (.getDeclaredField klass field-name)]
              (.setAccessible f true)
              (.get f datapoint)))]
    (cond-> {:count (get-private-val "count")
             :sum   (get-private-val "sum")}
            (= klass Histogram$DataPoint) (assoc :buckets (get-private-val "classicBuckets"))
            (= klass Summary$DataPoint) (assoc :quantiles (get-private-val "quantileValues")))))

(defn- start-timer* [^TimerApi datapoint]
  (let [^Timer t (.startTimer ^TimerApi datapoint)]
    #(.observeDuration t)))

;; ## Operation Protocols

(defprotocol ReadableCollector
  (read-value [this]))

(defprotocol IncrementableCollector
  (increment* [this amount]))

(defprotocol DecrementableCollector
  (decrement* [this amount]))

(defprotocol SettableCollector
  (set-value [this value])
  (set-value-to-current-time [this]))

(defprotocol ObservableCollector
  (observe [this amount]))

(defprotocol TimeableCollector
  (start-timer [this]))

;; ## Derived Functions

(defn increment
  ([this] (increment this 1.0))
  ([this amount] (increment* this amount)))

(defn decrement
  ([this] (decrement this 1.0))
  ([this amount] (decrement* this amount)))

;; ## Counter

(extend-type Counter$DataPoint
  ReadableCollector
  (read-value [this]
    (.get ^Counter$DataPoint this))
  IncrementableCollector
  (increment* [this amount]
    (.inc ^Counter$DataPoint this (double amount))))

;; ## Gauge

(extend-type Gauge$DataPoint
  ReadableCollector
  (read-value [this]
    (.get ^Gauge$DataPoint this))

  IncrementableCollector
  (increment* [this amount]
    (.inc ^Gauge$DataPoint this (double amount)))

  DecrementableCollector
  (decrement* [this amount]
    (.dec ^Gauge$DataPoint this (double amount)))

  ObservableCollector
  (observe [this amount]
    (.set ^Gauge$DataPoint this (double amount)))

  SettableCollector
  (set-value [this value]
    (.set ^Gauge$DataPoint this (double value)))
  (set-value-to-current-time [this]
    (.setToCurrentTime ^Gauge$Child this))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))

;; ## Histogram

(extend-type Histogram$DataPoint
  ReadableCollector
  (read-value [this]
    (get-distribution-values Histogram$DataPoint this)
    #_(let [^io.prometheus.client.Histogram$Child$Value value
            (.get ^Histogram$Child this)
            buckets (vec (.-buckets value))]
        {:sum     (.-sum value)
         :count   (last buckets)
         :buckets buckets}))

  ObservableCollector
  (observe [this amount]
    (.observe ^Histogram$DataPoint this (double amount)))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))

;; ## Summary

(extend-type Summary$Child
  ReadableCollector
  (read-value [this]
    (let [^io.prometheus.client.Summary$Child$Value value
          (.get ^Summary$Child this)]
      {:sum       (.-sum value)
       :count     (.-count value)
       :quantiles (into {} (.-quantiles value))}))

  ObservableCollector
  (observe [this amount]
    (.observe ^Summary$DataPoint this (double amount)))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))
