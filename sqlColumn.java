import java.util.List;
import java.util.HashMap;
import java.util.Map;
//import org.json.JSONObject;
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

    public Map<String, Object> getColumn(String sql) throws Exception {
        try {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            SQLStatement stmt = stmtList.get(0);

            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
            stmt.accept(statVisitor);

            Map<String, Object> dict = new HashMap<String, Object>();
            //JSONObject dict = new JSONObject();
            dict.put("tables", statVisitor.getTables().toString());
            dict.put("columns", statVisitor.getColumns().toString());
            dict.put("conditions", statVisitor.getConditions().toString());
            return dict;
        }

        catch (Exception e) {
            System.out.println("SOMETHING WRONG");
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        sqlColumn c1 = new sqlColumn("MYSQL");
        //Map r1 = c1.getColumn("UPDATE dwd_info_invest2 c,(SELECT ( @i := CASE WHEN @pre_part_no = key_account THEN @i + 1 ELSE 1 END ) row_number, dwd_info_invest.key_primary, ( @pre_part_no := key_account ) FROM dwd_info_invest, ( SELECT @i := 0, @pre_part_no := '' ) AS a WHERE status_invest <> 3 AND type_sourcetable IN ( 1, 2, 4 ) GROUP BY key_account,dt_invest,key_primary ORDER BY key_account ) b SET c.rank_partinvest = b.row_number WHERE c.key_primary = b.key_primary;");
        //JSONObject res_json = new JSONObject(r1);
        //System.out.println(r1);
        GatewayServer server = new GatewayServer(c1, 25334);
        server.start();
    }

}
