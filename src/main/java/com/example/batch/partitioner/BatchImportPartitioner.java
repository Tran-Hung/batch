package com.example.batch.partitioner;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
@Builder
public class BatchImportPartitioner implements Partitioner {
    private JdbcOperations jdbcTemplate;
    private String tableName;
    private String columnName;
    private String where;

    /**
     * The data source for connecting to the database.
     *
     * @param dataSource a {@link DataSource}
     */
    public BatchImportPartitioner dataSource(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
        return this;
    }

    /**
     * Partition a database table assuming that the data in the column specified are
     * uniformly distributed. The execution context values will have keys
     * <code>minValue</code> and <code>maxValue</code> specifying the range of
     * values to consider in each partition.
     *
     * @see Partitioner#partition(int)
     */
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<String, ExecutionContext>();

        String sql = "SELECT %s FROM ( SELECT ntile(%s) OVER(ORDER BY " + columnName + " ) AS PART , " + columnName +
                " FROM " + tableName + " ${where} GROUP BY  " + columnName + " ) TMP GROUP BY PART ORDER BY PART";

        sql = StringUtils.replace(sql, "${where}", StringUtils.defaultString(where, ""));

        // Split request by grid size with min/max of each block
        List<String> mins = jdbcTemplate.queryForList(String.format(sql, "MIN(" + columnName + ")", gridSize), String.class);
        List<String> maxs = jdbcTemplate.queryForList(String.format(sql, "MAX(" + columnName + ")", gridSize), String.class);
        if (!mins.isEmpty() && !maxs.isEmpty()) {
            for (int i = 0; i < mins.size(); i++) {
                ExecutionContext value = new ExecutionContext();
                value.putString("minValue", mins.get(i));
                value.putString("maxValue", maxs.get(i));
                value.putString("partitionName", String.valueOf(i + 1));
                result.put("partition" + i, value);
            }

        } else {
            log.warn(String.format("------------------- No record in %s ---------------------", tableName));
        }

        return result;
    }
}
