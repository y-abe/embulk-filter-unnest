
Gem::Specification.new do |spec|
  spec.name          = "embulk-filter-unnest"
  spec.version       = "0.1.0"
  spec.authors       = ["Yosuke Abe"]
  spec.summary       = %[Unnest filter plugin for Embulk]
  spec.description   = %[Unnest]
  spec.email         = ["y.abe.hep@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/y.abe.hep/embulk-filter-unnest"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['~> 12.0']
end
