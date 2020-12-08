/** @format */

module.exports = (modules = []) => {
  const collections = modules.map(model => require(`${model.path}/collections`)).reduce((o, instances) => ({ ...o, ...instances }), {})
  for (let key of Object.keys(collections)) {
    collections[key].useCollections()
  }
}
