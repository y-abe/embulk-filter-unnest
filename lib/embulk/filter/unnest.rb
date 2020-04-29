Embulk::JavaPlugin.register_filter(
  "unnest", "org.embulk.filter.unnest.UnnestFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
