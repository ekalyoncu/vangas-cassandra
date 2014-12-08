package net.vangas.cassandra

trait Factory[I,O] {
  def apply(input: I): O
}
