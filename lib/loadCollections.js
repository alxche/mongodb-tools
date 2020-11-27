/** @format */

module.exports = (modules = []) => {
  const collections = modules.map(name => require(`@api/${name}/collections`)).reduce((o, instances) => ({ ...o, ...instances }), {})
  for (let key of Object.keys(collections)) {
    collections[key].useCollections()
  }
}
