/** @format */

const { default: MongoObject } = require('mongo-object')
const _ = require('lodash')
const cache = require('./cache')
const types = require('typology')
const parsePhoneNumber = require('libphonenumber-js')

const castData = {
  firstName: data => {
    if (data.name) return data.name.split(' ')[0]
  },
  lastName: data => {
    if (data.name) return data.name.split(' ')[1]
  },
  name: data => {
    if (data.firstName) return `${data.firstName}${data.lastName && ' ' + data.firstName}`
  },
  phone: (data, v) => {
    let phone = parsePhoneNumber(v, 'US')
    return phone.format('E.164')
  }
}

class MongoCollection {
  constructor(name, database = null) {
    // Set cache
    this.cache = cache
    // Set DB
    this.db = global.db || database
    if (!this.db) {
      throw new Error('mongo instance db not found, you should declare db globally or pass to constructor')
    }

    // Set name collection
    this._name = name

    // Set collection
    this.collection = this.db.collection(name)

    // Set logger
    const debug = global.debug || require('debug')
    if (debug) {
      this.logger = debug(`app.${name}`)
      this.enableLog()
    } else {
      this.disableLog()
    }

    // Use collections
    this.collections = {}
  }

  async runSchema(schema, data) {
    let dataType = types.get(data)
    let sourceData = data
    let resultData = []
    if (dataType === 'object' && data.data && data.limit) sourceData = data.data
    else if (dataType !== 'array') sourceData = [sourceData]
    if (schema && schema.length > 0) {
      resultData = sourceData.map(d => {
        let consistentData = _.pick(d, schema.map(f => f.name))
        let rest = _.pick(d, _.difference(Object.keys(d), schema.map(f => f.name)))
        let error = false

        schema.forEach(f => {
          if (f.required === true && !_.has(consistentData, f.name)) {
            console.error(`${f.name} field is missing`)
            if (!f.cast || !castData[f.cast]) {
              if (!f.requiredException || Object.keys(_.pick(d, f.requiredException)).length < 1) error = true
            } else {
              if (castData[f.cast](d, consistentData[f.name])) consistentData[f.name] = castData[f.cast](d, consistentData[f.name])
              else error = true
            }
          }
          if (!error && f.type === 'date') {
            consistentData[f.name] = new Date(consistentData[f.name])
          }
          if (f.cast && castData[f.cast] && consistentData[f.name]) {
            if (castData[f.cast](d, consistentData[f.name])) consistentData[f.name] = castData[f.cast](d, consistentData[f.name])
            else error = true
          }
        })
        //if (error) return null
        return {
          ...consistentData,
          ...rest
        }
      })
    }
    resultData = _.filter(resultData, d => d !== null)
    if (dataType === 'object' && data.data && data.limit) {
      data.data = resultData
      return data
    } else if (dataType !== 'array') return resultData[0]
    return resultData
  }

  useCollections(instances = {}) {
    this.collections = {
      ...this.collections,
      ...instances
    }
  }

  async create(input = {}, context = {}) {
    const { user } = context
    let data = {
      ...input,
      createdAt: new Date()
    }
    if (user && user._id) {
      data.owner = user._id
    }

    data = await this.before(data, context)
    data = await this.beforeCreate(data, context)

    this.log('create %j', data)
    data = await this.useSchema(data)
    let doc = await this.insertOne(data)

    await this.after(doc, { ...context, data })
    await this.afterCreate(doc, { ...context, data })

    return this.get(doc._id, context)
  }

  async useSchema(data) {
    if (!data.id && data._id) data.id = data._id
    return data
  }

  async get(query = {}, context = {}) {
    const { resolveData, options = {} } = context
    let selector = typeof query === 'string' ? { _id: query } : { ...query }
    /*
    let doc = null

    if (Object.keys(selector).length > 1 || (!_.isEmpty(selector) && !selector._id)) {
      selector = await this.findOne(selector, { ...options, projection: { _id: 1 } })
    }

    if (_.isEmpty(selector)) {
      return null
    }

    doc = await this.findById(selector._id)
    */
    this.log('get %j', selector)

    if (_.isEmpty(selector)) {
      return null
    }

    const doc = await this.findOne(selector, options)

    if (doc) {
      return resolveData ? resolveData(doc, context) : this.useSchema(this.doc(doc, context))
    }
  }

  async search(query = {}, parentSelector = {}, context = {}) {
    const { resolveData, collection = '' } = context
    let { limit = 0, skip = 0, sort = [], ...rest } = query
    let selector = {}

    // selector
    Object.entries(rest || {}).forEach(([key, value]) => (selector[key] = this.formatQueryOperator(value)))
    selector = {
      ...selector,
      ...parentSelector
    }

    // sort
    if (sort && sort.length && ((!Array.isArray(sort) && sort.length) || (Array.isArray(sort) && !Array.isArray(sort[0])))) {
      //console.log('single',sort, Array.isArray(sort), sort.length)
      const [by, dir] = sort
      sort = { [by]: +dir || -1 }
    }

    if (sort && Array.isArray(sort) && sort.length > 2) {
      //console.log('multiple',sort, Array.isArray(sort), sort.length)
      let [by, dir] = sort[1]
      //sort = { [by]: +dir || -1 }
    }

    const options = { sort, skip: +skip, limit: +limit }

    this.log('find %j %j %s', selector, options, collection)

    const count = collection ? await this.db.collection(collection).countDocuments(selector) : await this.countDocuments(selector)
    let data = collection
      ? await this.db
        .collection(collection)
        .find(selector, options)
        .toArray()
      : await this.find(selector, options).toArray()
    data = await Promise.all(data.map(async doc => (resolveData ? resolveData(doc, context) : await this.doc(doc, context))))
    const pages = limit > 0 ? Math.ceil(count / limit) : 0
    data = await this.useSchema(data)
    return { data, skip: +skip, limit: +limit, sort, count, pages }
  }
  async save(query = {}, data = {}, context = {}) {
    const { options = {} } = context
    const selector = typeof query === 'string' ? { _id: query } : { ...query }
    let doc = null

    data = this.useSchema(data)

    doc = await this.get(selector, context)

    data = await this.before(data, { ...context, selector, doc })
    data = await this.beforeSave(data, { ...context, selector, doc })

    const modifier = MongoObject.docToModifier(
      { ...data, updatedAt: new Date() },
      {
        keepArrays: true,
        keepEmptyStrings: false,
        ..._.pick(options, 'keepArrays', 'keepEmptyStrings')
      }
    )

    this.log('save %j %j %j', selector, modifier, options)

    if (_.isEmpty(selector) || _.isEmpty(modifier)) {
      return null
    }

    doc = await this.findOneAndUpdate(selector, modifier, options)
    await this.after(doc, { ...context, data })
    await this.afterSave(doc, { ...context, data })

    //doc = await this.findOne({ _id: doc._id })
    //this.cache.set(`${this._name}.${doc._id}`, doc)

    return this.get(selector, context)
  }

  async copy(id) {
    const doc = await this.findOne({ _id: id })
    if (!doc) {
      throw new Error('Copy object not found')
    }
    if (doc) {
      return _.omit(doc, '_id', 'id', 'createdAt', 'updatedAt', 'owner')
    }
  }

  async remove(query = {}, context = {}) {
    const { options = {} } = context
    let selector = typeof query === 'string' ? { _id: query } : query
    let doc = null

    this.log('remove %j', selector)

    if (_.isEmpty(selector)) {
      return null
    }

    doc = await this.get(selector, context)
    if (_.isEmpty(doc)) {
      return null
    }
    await this.beforeRemove(doc, context)
    doc = await this.findOneAndDelete({ _id: doc._id }, options)
    await this.afterRemove(doc, context)

    //this.cache.del(`${this._name}.${doc._id}`)

    return this.withId(doc)
  }

  async removeMany(query = {}, context) {
    this.log('removeMany %j', query)
    const result = await this.deleteMany(query)
    return result
  }

  async findAllById(ids = []) {
    let result = []
    let doc = null
    for (let id of ids) {
      doc = await this.findById(id)
      if (doc) {
        result.push(doc)
      }
    }
    return result
  }

  async findById(id) {
    let doc = null //id ? this.cache.get(`${this._name}.${id}`) : null
    if (id && !doc) {
      this.log('get %j', { _id: id })
      doc = await this.findOne({ _id: id })
      if (doc) {
        //this.cache.set(`${this._name}.${id}`, doc)
      }
    }
    return doc
  }

  find(query, options) {
    return this.collection.find(query, options).map(this.withId)
  }
  async findOne(filter, options) {
    const result = await this.collection.findOne(filter, options)
    return this.withId(result)
  }
  async findOneAndDelete(filter, options) {
    const result = await this.collection.findOneAndDelete(filter, options)
    return this.withId(result.value)
  }
  async findOneAndReplace(filter, replacement, options) {
    const result = await this.collection.findOneAndReplace(filter, replacement, options)
    return this.withId(result.value)
  }
  async findOneAndUpdate(filter, data, options) {
    const result = await this.collection.findOneAndUpdate(filter, data, {
      returnOriginal: false,
      ...options
    })
    return this.withId(result.value)
  }

  async insertOne(docs, options) {
    const result = await this.collection.insertOne(docs, options)
    return this.withId(result.ops[0])
  }
  async insertMany(docs, options) {
    const result = await this.collection.insertMany(docs, options)
    return result.ops.map(this.withId)
  }
  async replaceOne(filter, doc, options) {
    const result = await this.collection.replaceOne(filter, doc, options)
    return this.withId(result.ops[0])
  }

  async updateOne(filter, update, options) {
    let doc = null
    await this.collection.updateOne(filter, update, options)
    doc = await this.findOne(filter)
    if (doc) {
      //this.cache.set(`${this._name}.${doc._id}`, doc)
    }
    return doc
  }
  async updateMany(filter, update, options) {
    await this.collection.updateMany(filter, update, options)
  }
  async deleteOne(...args) {
    return this.collection.deleteOne(...args)
  }
  async deleteMany(...args) {
    return this.collection.deleteMany(...args)
  }

  async countDocuments(...args) {
    return this.collection.countDocuments(...args)
  }
  async estimatedDocumentCount(...args) {
    return this.collection.estimatedDocumentCount(...args)
  }
  async distinct(...args) {
    return this.collection.distinct(...args)
  }

  aggregate(...args) {
    return this.collection.aggregate(...args)
  }
  async bulkWrite(...args) {
    return this.collection.bulkWrite(...args)
  }

  async createIndex(...args) {
    return this.collection.createIndex(...args)
  }
  async createIndexes(...args) {
    return this.collection.createIndexes(...args)
  }
  async indexes(...args) {
    return this.collection.indexes(...args)
  }
  async indexExists(...args) {
    return this.collection.indexExists(...args)
  }
  async indexInformation(...args) {
    return this.collection.indexInformation(...args)
  }
  listIndexes(...args) {
    return this.collection.listIndexes(...args)
  }
  async reIndex(...args) {
    return this.collection.reIndex(...args)
  }
  async dropIndex(...args) {
    return this.collection.dropIndex(...args)
  }
  async dropIndexes(...args) {
    return this.collection.dropIndexes(...args)
  }

  async rename(...args) {
    return this.collection.rename(...args)
  }
  async stats(...args) {
    return this.collection.stats(...args)
  }
  async options(...args) {
    return this.collection.options(...args)
  }
  async isCapped(...args) {
    return this.collection.isCapped(...args)
  }
  async drop(...args) {
    return this.collection.drop(...args)
  }

  async before(data, context) {
    return data
  }
  async beforeCreate(data, context) {
    return data
  }
  async beforeSave(doc, context) {
    return doc
  }
  async beforeRemove(doc, context) {
    return doc
  }

  async after(doc, context) {
    return true
  }
  async afterCreate(doc, context) {
    return true
  }
  async afterSave(doc, context) {
    return true
  }
  async afterRemove(doc, context) {
    return true
  }

  log(...args) {
    if (this.logging && this.logger) {
      console.log(...args)
      this.logger(...args)
    }
  }

  enableLog() {
    this.logging = true
  }

  disableLog() {
    this.logging = false
  }

  doc(doc, context) {
    return {
      ...doc,
      id: doc._id
    }
  }

  withId(doc) {
    return doc ? { ...doc, id: doc._id } : null
  }

  formatFields(fields = '') {
    return String(fields)
      .split(/[\s,]/)
      .map(v => v.trim())
      .filter(v => !!v)
      .reduce((o, a) => ({ ...o, [a]: 1 }), {})
  }

  formatQueryOperator(operator) {
    if (_.isPlainObject(operator)) {
      return _.transform(operator, (r, v, k) => {
        r[`$${k}`] = v
      })
    }
    return operator
  }
}

module.exports = MongoCollection
