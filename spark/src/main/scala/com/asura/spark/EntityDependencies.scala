package com.asura.spark

/**
 * @author asura7969
 * @create 2021-11-07-12:26
 */
class EntityDependencies(entity: Entity, val input: Entity, val output: Entity) {

  private var next: EntityDependencies = _

  def addDependencies(deps: EntityDependencies): Unit = next = deps

  def getNext: EntityDependencies = next
}

object EntityDependencies {
  def apply(entity: Entity,
            input: Entity,
            output: Entity): EntityDependencies =
    new EntityDependencies(entity, input, output)

  def apply(entity: Entity): EntityDependencies = new EntityDependencies(entity, null, null)
}
