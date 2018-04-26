import py4j.GatewayServer
import com.alibaba.druid.sql.SQLUtils
import scala.collection.JavaConverters._


class sqlGetColumn(sql_type: String) {

  private val type_class = Class.forName("com.alibaba.druid.util.JdbcConstants")
  val dbType = type_class.getField(sql_type).get(null).toString()

  def get_column(sql: String): java.util.Map[String,java.util.List[String]] = {
    val stmtList = SQLUtils.parseStatements(sql, dbType)
    val stmt = stmtList.get(0)
    val statVisitor = SQLUtils.createSchemaStatVisitor(dbType)
    stmt.accept(statVisitor)
    Map(
      "tables" -> statVisitor.getTables().keySet().asScala.map(x => x.toString).toList.asJava,
      "columns" -> statVisitor.getColumns().asScala.map(x => x.toString).toList.asJava
    ).asJava
  }

}

object sqlColumn {
  def main(args: Array[String]): Unit = {
    val obj = new sqlGetColumn("MYSQL")
    val server = new GatewayServer(obj, 25336)
    server.start()
    //val testsql = "UPDATE dwd_info_invest2 c,(SELECT ( @i := CASE WHEN @pre_part_no = key_account THEN @i + 1 ELSE 1 END ) row_number, dwd_info_invest.key_primary, ( @pre_part_no := key_account ) FROM dwd_info_invest, ( SELECT @i := 0, @pre_part_no := '' ) AS a WHERE status_invest <> 3 AND type_sourcetable IN ( 1, 2, 4 ) GROUP BY key_account,dt_invest,key_primary ORDER BY key_account ) b SET c.rank_partinvest = b.row_number WHERE c.key_primary = b.key_primary;"
    //val res1 = obj.get_column(testsql)
    //println(res1)
  }
}
