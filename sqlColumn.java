import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import py4j.GatewayServer;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;


public class sqlColumn {

    private final String dbType;

    public sqlColumn(String sqltype) throws Exception {
        Class classType = Class.forName("com.alibaba.druid.util.JdbcConstants");
        String dbType = classType.getField(sqltype).get(null).toString();   //数据库类型
        this.dbType = dbType;
    }

    public Map<String, List> getColumn(String sql) throws Exception {
        try {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            SQLStatement stmt = stmtList.get(0);
            
            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
            stmt.accept(statVisitor);
            
            Map<String, List> dict = new HashMap<String, List>();
            dict.put("tables", statVisitor.getTables().keySet().stream().map(x -> x.toString()).collect(Collectors.toList()));
            dict.put("columns", statVisitor.getColumns().stream().map(x -> x.toString()).collect(Collectors.toList()));
            dict.put("conditions", statVisitor.getConditions().stream().map(x -> x.toString()).collect(Collectors.toList()));
            return dict;
        } 
        
        catch (Exception e) {
            System.out.println("SOMETHING WRONG");
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        sqlColumn obj = new sqlColumn("MYSQL");
        GatewayServer server = new GatewayServer(obj, 25335);
        server.start();
        /*String testsql = "update dwd_info_invest2 c,\n" +
                "\t\t(select \n" +
                "        (@i := case when @pre_part_no=key_account then @i + 1 else 1 end) row_number,     \n" +
                "\t\t\t\tdwd_info_invest.key_primary,\n" +
                "        (@pre_part_no:=key_account)        \n" +
                "    from dwd_info_invest,\n" +
                "    (SELECT @i := 0, @pre_part_no:='') AS a\n" +
                "\t  where status_invest <> 3\n" +
                "    group by key_account, dt_invest, key_primary  \n" +
                "    order by key_account) b\n" +
                "set c.rank_allinvest = b.row_number\n" +
                "where c.key_primary = b.key_primary;";
        System.out.println(obj.getColumn(testsql));
        */
    }

}
