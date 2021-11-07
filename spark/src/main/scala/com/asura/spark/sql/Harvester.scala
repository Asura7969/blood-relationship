package com.asura.spark.sql

import com.asura.spark.EntityDependencies

trait Harvester[T] {
  def harvest(node: T, qd: QueryDetail): EntityDependencies
}
