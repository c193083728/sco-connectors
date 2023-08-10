def fileContent = project.properties.findAll {
    key, value -> key.startsWith('container.image')
}.collect {
    key, value -> "${key}=${value}"
}.join('\n')

File testClasses = new File("${project.build.outputDirectory}")
println testClasses
if (testClasses.exists()) {
    File file = new File("${testClasses.absolutePath}/containers.properties")
    file.write(fileContent)
}