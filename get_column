import java.util.List;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;


public class get_column {
    public static void main(String[] args) throws Exception {

        // String sql = "update t set name = 'x' where id < 100 limit 10";
        // String sql = "SELECT ID, NAME, AGE FROM USER WHERE ID = ? limit 2";
        // String sql = "select * from tablename limit 10";

        String arg_sql = "";
        if (args.length != 0) {
            for (int i = 0; i < args.length; i++)
                arg_sql += String.format("%s ", args[i]);
        }

        String sql = arg_sql;
        System.out.println(sql);

        String dbType = JdbcConstants.MYSQL;

        try {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            SQLStatement stmt = stmtList.get(0);

            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
            stmt.accept(statVisitor);
            System.out.println(statVisitor.getTables()); // {t_user=Select}
            System.out.println(statVisitor.getColumns()); // [t_user.name, t_user.age, t_user.id]
            System.out.println(statVisitor.getConditions()); // [t_user.id = 1]
        }
        catch (Exception e){
            System.out.println("something bad");
            throw e;
        }
    }
}
