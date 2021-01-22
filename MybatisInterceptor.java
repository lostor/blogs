
import com.github.pagehelper.BoundSqlInterceptor;
import com.github.pagehelper.util.ExecutorUtil;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Intercepts(
        {
                @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
                @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        }
)
public class HistoryInterceptor implements Interceptor {

    private Logger log = LoggerFactory.getLogger(HistoryInterceptor.class);
    private static final String ORDER_TABLE = "tb_a";
    private static final String FQ_TABLE = "tb_b";
    private static final String SUFFIX = "_history";

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        try {
            Object[] args = invocation.getArgs();
            MappedStatement ms = (MappedStatement) args[0];
            Object parameter = args[1];
            if (null == parameter) {
                return invocation.proceed();
            }
            RowBounds rowBounds = (RowBounds) args[2];
            ResultHandler resultHandler = (ResultHandler) args[3];
            Executor executor = (Executor) invocation.getTarget();

            CacheKey cacheKey;
            BoundSql boundSql;
            //由于逻辑关系，只会进入一次
            if (args.length == 4) {
                //4 个参数时
                boundSql = ms.getBoundSql(parameter);
                cacheKey = executor.createCacheKey(ms, parameter, rowBounds, boundSql);
            } else {
                //6 个参数时
                cacheKey = (CacheKey) args[4];
                boundSql = (BoundSql) args[5];
            }

            if (!ms.getId().startsWith("co.pinguan.pms.dao.OrderDao") && !ms.getId().startsWith("co.pinguan.pms.dao.FqDao")) {
                return invocation.proceed();
            }

            // 获取sql
            String sql = boundSql.getSql();

            log.debug("source:" + sql);

            if (sql.indexOf(ORDER_TABLE) == -1 && sql.indexOf(FQ_TABLE) == -1) {
                return invocation.proceed();
            }

            if (StringUtils.isBlank(sql)) {
                return invocation.proceed();
            }
            //========================================================分表处理关键位置 start===============================================================
            //根据需求 替换表
            sql = sql.replaceAll(ORDER_TABLE, ORDER_TABLE + SUFFIX);
            sql = sql.replaceAll(FQ_TABLE, FQ_TABLE + SUFFIX);
            //========================================================分表处理关键位置 end===============================================================
            BoundSql bs = new BoundSql(ms.getConfiguration(), sql, boundSql.getParameterMappings(), boundSql.getParameterObject());
            log.debug("reset:" + sql);
            //args[0] = newStatement;
            if (args.length == 4) {
            } else {
                //6 个参数时
                args[5] = bs;
            }
            return invocation.proceed();
        } finally {
        }
    }

}
