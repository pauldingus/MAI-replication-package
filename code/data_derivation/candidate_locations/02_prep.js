// IMPORT DIFF IMG
var cellNumber = INSERT_CELL_NUMBER_HERE
var countryName = "INSERT_COUNTRYNAME_HERE"
var path = "INSERT_PATH_HERE"
var countryNameString = countryName.replace(/ /g, '') 
var projectName = countryNameString.toLowerCase()
var i = ee.Image(path+'/S2/diffImgs/cell_'+cellNumber).select('max_all')
// Map.addLayer(i, {max:20, bands: ['max_all']}, 'image 1')

// CREATE A COVERING GRID, GET SD AND MEAN FOR EACH CELL
var mask = i.mask().int()
var cells = i.geometry().coveringGrid(i.projection(), 5000)
cells = ee.Image(mask).reduceRegions({
  collection: cells, 
  reducer: ee.Reducer.sum(), 
  scale: 10, 
  crs: mask.projection()
}).filter(ee.Filter.gt('sum', 0)).filterBounds(i.geometry().buffer(-100))
//Map.addLayer(cells, {color:'orange'}, 'cells to process')

var sds  = i
  .reduceRegions({
    reducer: ee.Reducer.stdDev().combine({reducer2: ee.Reducer.mean(), sharedInputs: true}), 
    collection: cells,
    scale: 10
  })

// STANDARDIZE IMAGE WITHIN EACH GRID CELL
var icNormalized = sds.map(function(cell){
  var iCell = i.clip(cell.geometry())
  var mean = ee.Number(cell.get('mean'))
  var stdDev = ee.Number(cell.get('stdDev'))
  var iCellNorm = iCell.subtract(mean).divide(stdDev)
  return iCellNorm.set('mean', mean, 'stdDev', stdDev)
})
icNormalized = ee.ImageCollection(icNormalized)

var iNormalized = icNormalized.reduce(ee.Reducer.mean()).rename('max_all_norm') //reduce(ee.Reducer.sum())

// EXPORT NORMALIZED IMAGE
var params = {
  element: iNormalized,
  type:'EXPORT_IMAGE',
  description: 'S2diffImg_norm_' + countryNameString + cellNumber,
  region: i.geometry(),
  scale: 10,
  maxPixels: 1e13,
  assetId: path+'/S2/diffImgs/cell_'+cellNumber+'_norm5k'
}

var taskId = ee.data.newTaskId(1)
ee.data.startProcessing(taskId, params);