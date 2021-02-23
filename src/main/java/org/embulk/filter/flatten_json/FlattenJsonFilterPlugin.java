package org.embulk.filter.flatten_json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

public class FlattenJsonFilterPlugin implements FilterPlugin {
  private static final Logger logger = Exec.getLogger(FlattenJsonFilterPlugin.class);

  public interface PluginTask extends Task {
    @Config("json_columns")
    public List<String> getJsonColumns();

    @Config("separator")
    @ConfigDefault("\",\"")
    public String getSeparator();

    @Config("array_index_prefix")
    @ConfigDefault("null")
    public Optional<String> getArrayIndexPrefix();
  }

  @Override
  public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
    PluginTask task = config.loadConfig(PluginTask.class);

    Schema outputSchema = inputSchema;

    control.run(task.dump(), outputSchema);
  }

  @Override
  public PageOutput open(
      TaskSource taskSource,
      final Schema inputSchema,
      final Schema outputSchema,
      final PageOutput output) {
    final PluginTask task = taskSource.loadTask(PluginTask.class);

    final List<Column> inputColumns = inputSchema.getColumns();
    final List<Column> inputColumnsExceptFlattenJsonColumns = new ArrayList<>();
    final List<Column> flattenJsonColumns = new ArrayList<>();
    for (Column inputColumn : inputColumns) {
      if (task.getJsonColumns().contains(inputColumn.getName())) {
        flattenJsonColumns.add(inputColumn);
      } else {
        inputColumnsExceptFlattenJsonColumns.add(inputColumn);
      }
    }

    return new PageOutput() {
      private PageReader pageReader = new PageReader(inputSchema);
      private PageBuilder pageBuilder =
          new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

      @Override
      public void add(Page page) {
        try {
          pageReader.setPage(page);

          while (pageReader.nextRecord()) {
            setInputColumnsExceptFlattenJsonColumns(
                pageBuilder, inputColumnsExceptFlattenJsonColumns);
            setFlattenJsonColumns(
                pageBuilder, flattenJsonColumns, task.getSeparator(), task.getArrayIndexPrefix());
            pageBuilder.addRecord();
          }
        } catch (IOException e) {
          logger.error(e.getMessage());
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void finish() {
        pageBuilder.finish();
      }

      @Override
      public void close() {
        pageReader.close();
        output.close();
      }

      private void setInputColumnsExceptFlattenJsonColumns(
          PageBuilder pageBuilder, List<Column> inputColumnsExceptFlattenJsonColumns) {
        for (Column inputColumn : inputColumnsExceptFlattenJsonColumns) {
          if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(inputColumn);
            continue;
          }

          if (Types.STRING.equals(inputColumn.getType())) {
            pageBuilder.setString(inputColumn, pageReader.getString(inputColumn));
          } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
            pageBuilder.setBoolean(inputColumn, pageReader.getBoolean(inputColumn));
          } else if (Types.DOUBLE.equals(inputColumn.getType())) {
            pageBuilder.setDouble(inputColumn, pageReader.getDouble(inputColumn));
          } else if (Types.LONG.equals(inputColumn.getType())) {
            pageBuilder.setLong(inputColumn, pageReader.getLong(inputColumn));
          } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
            pageBuilder.setTimestamp(inputColumn, pageReader.getTimestamp(inputColumn));
          }
        }
      }

      private void setFlattenJsonColumns(
          PageBuilder pageBuilder,
          List<Column> flattenJsonColumns,
          String separator,
          Optional<String> arrayIndexPrefix)
          throws IOException {
        for (Column flattenJsonColumn : flattenJsonColumns) {
          if (pageReader.isNull(flattenJsonColumn)) {
            pageBuilder.setNull(flattenJsonColumn);
            continue;
          }

          ObjectMapper objectMapper = new ObjectMapper();
          String json = pageReader.getString(flattenJsonColumn);
          JsonNode jsonNode = objectMapper.readTree(json);

          Map<String, String> jsonMap = Maps.newHashMap();
          joinNode("", jsonNode, jsonMap, separator, arrayIndexPrefix);

          String flattenedJson = objectMapper.writeValueAsString(jsonMap);

          pageBuilder.setString(flattenJsonColumn, flattenedJson);
        }
      }

      private void joinNode(
          String currentPath,
          JsonNode jsonNode,
          Map<String, String> map,
          String separator,
          Optional<String> arrayIndexPrefix) {
        if (jsonNode.isObject()) {
          ObjectNode objectNode = (ObjectNode) jsonNode;
          Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
          String pathPrefix = currentPath.isEmpty() ? "" : currentPath + separator;

          while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            joinNode(
                pathPrefix + entry.getKey(), entry.getValue(), map, separator, arrayIndexPrefix);
          }
        } else if (jsonNode.isArray()) {
          ArrayNode arrayNode = (ArrayNode) jsonNode;
          if (arrayIndexPrefix.isPresent()) {
            for (int i = 0; i < arrayNode.size(); i++) {
              joinNode(
                  currentPath + separator + arrayIndexPrefix.get() + i,
                  arrayNode.get(i),
                  map,
                  separator,
                  arrayIndexPrefix);
            }
          } else {
            for (int i = 0; i < arrayNode.size(); i++) {
              joinNode(
                  currentPath + "[" + i + "]", arrayNode.get(i), map, separator, arrayIndexPrefix);
            }
          }
        } else if (jsonNode.isValueNode()) {
          ValueNode valueNode = (ValueNode) jsonNode;
          map.put(currentPath, valueNode.asText());
        }
      }
    };
  }
}
