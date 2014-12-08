package net.vangas.cassandra

import akka.util.{ByteIterator, ByteString}
import net.vangas.cassandra.util.ByteUtils._

case class ColumnDefinition(keyspaceName: String, tableName: String, name: String, dataType: DataType[_])

case class MetaData(globalTableSpec: Boolean,
                    hasMorePages: Boolean,
                    noMetaData: Boolean,
                    columnsCount: Int,
                    pagingState: Option[ByteString],
                    keyspaceName: String,
                    tableName: String,
                    columns: IndexedSeq[ColumnDefinition])

object MetaData extends Factory[ByteIterator, MetaData]{

  def apply(data: ByteIterator): MetaData = {
    val flag = data.getInt
    val globalTableSpec = (flag & 1) != 0
    val hasMorePages = (flag & 2) != 0
    val noMetaData = (flag & 4) != 0
    val columnsCount = data.getInt

    val pagingState =
      if (hasMorePages) {
        readByteString(data)
      } else {
        None
      }

    val (keyspaceName, tableName) =
      if (globalTableSpec) {
        (readString(data), readString(data))
      } else {
        (null, null)
      }

    val columns: IndexedSeq[ColumnDefinition] = 0 until columnsCount map { _ =>
      val (columnKeyspaceName, columnTableName) =
        if (globalTableSpec) (keyspaceName, tableName) else (readString(data), readString(data))
      val name = readString(data)
      val dataType = DataType(data)

      ColumnDefinition(columnKeyspaceName, columnTableName, name, dataType)
    }

    MetaData(globalTableSpec, hasMorePages, noMetaData, columnsCount, pagingState, keyspaceName, tableName, columns)
  }
}
