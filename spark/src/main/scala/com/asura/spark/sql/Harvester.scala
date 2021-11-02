package com.asura.spark.sql

trait Harvester[T] {
  def harvest(node: T, qd: QueryDetail): Unit
}
