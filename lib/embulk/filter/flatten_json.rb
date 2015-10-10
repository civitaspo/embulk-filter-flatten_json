Embulk::JavaPlugin.register_filter(
  "flatten_json", "org.embulk.filter.flatten_json.FlattenJsonFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
