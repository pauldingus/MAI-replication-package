// IMPORT DIFF IMG
var cellNumber = INSERT_CELL_NUMBER_HERE
var countryName = "INSERT_COUNTRYNAME_HERE"
var path = "INSERT_PATH_HERE"
// var cellNumber = 10
// var countryName = 'Uganda'
var countryNameString = countryName.replace(/ /g, '') 
var projectName = countryNameString.toLowerCase()

var grid = ee.FeatureCollection(path+"/cells_"+countryNameString)
var cell = ee.Feature(grid.toList(grid.size().getInfo()).get(cellNumber)).geometry()

var totalMask = ee.FeatureCollection(path+'/builtupMask')
  .filterBounds(cell)
  
var processingArea = totalMask.geometry().intersection(cell)
// Map.addLayer(processingArea, {}, 'processingArea', false)

var iNormalized = ee.Image(path+'/S2/diffImgs/cell_'+cellNumber+'_norm5k')
// Map.addLayer(iNormalized, {max:15}, 'iNormalized')

// FIND GROUPS OF PIXELS ABOVE CUTOFF 8
var cutoffMask = iNormalized.gt(8).selfMask().rename('high_signal')
// Map.addLayer(cutoffMask, {palette: 'FF0000'}, 'cutoffMask');

var objectId = cutoffMask.connectedComponents({
  connectedness: ee.Kernel.circle(35, 'meters'),
  maxSize: 128
})

// CALCULATE THE MAX SIGNAL IN EACH GROUP
iNormalized = objectId.select('labels').addBands(iNormalized);

// var maxSignal = iNormalized.reduceConnectedComponents({
//   reducer: ee.Reducer.max(),
//   labelBand: 'labels'
// });
//print('maxSignal', maxSignal)


// CONVERT TO A FEATURE COLLECTION
var maxSignalFC = iNormalized.reduceToVectors({
  reducer: ee.Reducer.max(),
  geometry: cell,
  scale: 10,
  labelProperty: 'labels',
  eightConnected: true,
  crs: iNormalized.projection(),
  maxPixels: 1e13
})
// print('maxSignalFC_1', maxSignalFC)


// JOIN FEATURES BASED ON OBJECT LABEL
var maxSignalFC = ee.Join.saveAll('sameLabel')
  .apply(maxSignalFC.distinct('labels'), 
    maxSignalFC.limit(maxSignalFC.size()), 
    ee.Filter.equals({leftField: 'labels', rightField: 'labels'}))
  .map(function(f){
    var fc = ee.FeatureCollection(ee.List(f.get('sameLabel')))
    var max = fc.aggregate_max('max')
    return f.setGeometry(fc.geometry()).set('maxSignal', max)
  })
// print('maxSignalFC_2', maxSignalFC)

maxSignalFC = maxSignalFC.map(function(f){
  return f.set('area', f.area(20))
})
// print('maxSignalFC_3', maxSignalFC)
// Map.addLayer(maxSignalFC, {}, 'maxSignalFC_2')


// FILTER BASED ON AREA AND CALCULATE TOTAL SIGNAL
maxSignalFC = maxSignalFC
  .filter(ee.Filter.gt('area', 450))
  .map(function(f){
    var i = iNormalized.updateMask(iNormalized.gt(5))
    var totalSignal = i.reduceRegion({
      reducer: ee.Reducer.sum(), 
      geometry: f.geometry().buffer(30, 1), 
      scale: 10, 
      crs: i.projection()
    }).get('max_all_norm')
    return(f.set('totalSignal', totalSignal))
  })
// print('maxSignalFC_4', maxSignalFC)
// Map.addLayer(maxSignalFC, {color:'blue'}, 'maxSignalFC_3')


// FILTER TO ONLY BUILT UP AREAS
maxSignalFC = maxSignalFC
  .filterBounds(processingArea)
  .select('area', 'labels', 'maxSignal', 'totalSignal')
//print('maxSignalFC_5', maxSignalFC)
//Map.addLayer(maxSignalFC, {color: 'cyan'}, 'totalSignalFC')

// BUFFER AND SPLIT UP OVERLAPPING LOCS
var locsInitial = maxSignalFC.map(function(f){return f.buffer(250)})

var overlappingLocs = locsInitial.map(function(f){
  var numOverlapping = locsInitial.filterBounds(f.geometry()).size()
  return(f.set('numOverlapping', numOverlapping))
}).filter(ee.Filter.gt('numOverlapping', 1))
// print(overlappingLocs)

//function to split overlapping locs
var locHandler = function(currentFeature, locList){
  locList = ee.List(locList)
  var locs = ee.FeatureCollection(locList);
  
  //return null if signal is already fully contained in a loc
  var alreadyContained = locs.geometry().contains(currentFeature.geometry(), 5)
  var loc = ee.Algorithms.If(alreadyContained, 
      ee.Feature(null), 
      currentFeature.buffer(250).simplify(10).difference(locs.geometry()))
  loc = ee.Feature(loc)
  
  //buffer signal inside and outside the loc, so that 
  //no signal cluster is closer than 100m to a loc border
  var signalBuffered100outside = maxSignalFC
    .filterBounds(loc.geometry().buffer(100))
    .filter(ee.Filter.intersects(".geo", loc.geometry()).not())
    .geometry().buffer(100).simplify(10)
  var signalBuffered100inside = maxSignalFC
    .filterBounds(loc.geometry())
    .geometry().buffer(100).simplify(10)
  var finalGeo = loc.geometry()
    .difference(signalBuffered100outside)
    .union(signalBuffered100inside)
  loc = ee.Feature(loc.setGeometry(finalGeo))
  return locList.add(loc)
}

//split overlapping locs in order from highest signal to lowest
var first = ee.List([ee.Feature(null, {first: true})])
var featuresToProcess = maxSignalFC.filterBounds(overlappingLocs.geometry())
var overlappingLocsFixed = ee.FeatureCollection(ee.List(featuresToProcess.sort('maxSignal', false)
  .iterate(locHandler, first)))
  .filter(ee.Filter.notNull([".geo"]))
// print(overlappingLocsFixed)

//replace original overlapping locs
var candidateLocs = locsInitial
  .filter(ee.Filter.intersects(".geo", overlappingLocsFixed.geometry()).not())
  .merge(overlappingLocsFixed)
  .filterBounds(locsInitial.geometry().bounds()) //get rid of null geometries
// print(candidateLocs)
// Map.addLayer(candidateLocs, {color:'cyan'}, 'candidateLocs')
// Map.centerObject(candidateLocs)

//EXPORT
var params={
  element: candidateLocs,
  type: 'EXPORT_FEATURES',
  description: countryNameString+'_locs_cell'+cellNumber,
  assetId: path+'/S2/locs/cell_'+cellNumber+'_locs_v20240812'
}

var taskIdAsset = ee.data.newTaskId(1);
ee.data.startProcessing(taskIdAsset, params);
