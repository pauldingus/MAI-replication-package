// look at image counts, ndvi as possible sources of noise

// Function to add the polar corrdinate bands to each image
function polarCoor_Conversion(image){
  var temp_r = image.select('B4');
  var temp_g = image.select('B3');
  var temp_b = image.select('B2');
  
  var temp_rho = temp_r.pow(2).add(temp_g.pow(2)).add(temp_b.pow(2)).pow(0.5).rename('rho');
  var temp_theta1 = temp_r.pow(2).add(temp_g.pow(2)).pow(0.5).divide(temp_b).atan().rename('t1');
  var temp_theta2 = temp_g.divide(temp_r).atan().rename('t2');
  
  return image.addBands(temp_rho).addBands(temp_theta1).addBands(temp_theta2);
}

// IMPORTS
var cellNumber = INSERT_CELL_NUMBER_HERE
var countryName = "INSERT_COUNTRYNAME_HERE"
var path = "INSERT_PATH_HERE"
// var cellNumber = 1
// var countryName = "Burkina Faso Test"
var countryNameString = countryName.replace(/ /g, '') 
var projectName = countryNameString.toLowerCase()

var grid = ee.FeatureCollection(path+"/cells_"+countryNameString)
var cell = ee.Feature(grid.toList(grid.size().getInfo()).get(cellNumber)).geometry()
var builtupMask = ee.FeatureCollection(path+"/builtupMask")
  .filterBounds(cell)
  .geometry()
  //.dissolve()
//convert this to an image first? -- try it at least
  
var water = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
  .filterBounds(cell)
  .filterDate('2022-01-01', '2024-01-01')
  .select(['water']).mean()
//filter dynamic world to only last two years

var waterMask = water.lt(0.3)
var builtup_mask_image_buffered = ee.Image.constant(1).clip(builtupMask).mask().multiply(waterMask)
//Map.addLayer(cell, {}, 'cell to process')

var visParams = {
  min: 0,
  max: 3000,
  bands: ['B4', 'B3', 'B2'],
};

// IMPORT SENTINEL DATA 
var S2 = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
  //.filterDate('2017-01-01', '2024-05-31')
  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 80))
  .filterBounds(cell)
  //.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50))
  .select('B2', 'B3', 'B4', 'B8', 'QA60', 'MSK_CLDPRB')
  //.limit(5000)
// print('S2', S2)  
// Map.addLayer(S2.first(), visParams, 'non-masked image')
// Map.addLayer(S2.first().select('QA60'), {max:1}, 'cld mask')
// Map.addLayer(S2.first().select('MSK_CLDPRB').lt(0.5), {max:1}, 'prob mask')

var S2polar = S2.map(function(i){
  var iPolar = polarCoor_Conversion(i).updateMask(builtup_mask_image_buffered)
  return iPolar.updateMask(iPolar.select('MSK_CLDPRB').lt(10))//.updateMask(i.select('QA60').not())
})
// print('S2polar', S2polar)
// Map.addLayer(S2polar.first(), visParams, 'cloud masked image')
// Map.addLayer(S2.first().select('MSK_CLDPRB'), {max:100}, 'cloud prob')
  
  
// GET WEEKDAY FOR EACH IMAGE
var S2polarWd = S2polar.map(function(i){
  var wd = ee.Date(i.get('system:time_start')).getRelative('day','week')
  var m = ee.Date(i.get('system:time_start')).get('month')
  wd = wd.add(ee.Number(8)).mod(7)
  return i.set('weekday', wd, 'month', m)
})
//print('S2polarWd', S2polarWd)
// print('date',ee.Date(S2wd.first().get('system:time_start')))
// print('weekday', S2wd.first().get('weekday'))


// CREATE COMPOSITE FOR EACH MONTH
var monthList = ee.List([1,2,3,4,5,6,7,8,9,10,11,12])

var compositesPerMonth = monthList.map(function(m){
  
  var icMonth = S2polarWd.filter(ee.Filter.eq('month', m))
  
  var totalComposite = icMonth
    .select(['B2','B3','B4', 't1', 't2', 'B8'])
    .reduce(ee.Reducer.intervalMean({minPercentile:20, maxPercentile:80, maxRaw:1000}))
    //.reduce(ee.Reducer.mean()) //using mean to try to increase speed
    .rename(['B2','B3','B4', 't1', 't2', 'B8'])
    
  return totalComposite.set('month', m)
})
compositesPerMonth = ee.ImageCollection(compositesPerMonth)

// GET MONTHLY NDVI INDEX TO MASK OUT VEGETATION
var monthlyNDVI = compositesPerMonth.map(function(image){
  var nir = image.select('B8');
  var red = image.select('B4');
  var ndvi = nir.subtract(red).divide(nir.add(red)).rename('NDVI');
  return ndvi.copyProperties(image);
});
var monthlyNDVI_max = monthlyNDVI.reduce(ee.Reducer.max());
// print('monthlyNDVI_max', monthlyNDVI_max);
// Map.addLayer(monthlyNDVI_max, {min:0, max:1}, 'monthlyNDVI_max');

var NDVI_mask = monthlyNDVI_max.lt(0.7)
// Map.addLayer(NDVI_mask, {min:0, max:1}, 'NDVI_mask');

// CREATE DIFFIMG FOR EACH IMAGE
var diffImgs = S2polarWd.map(function(i){
  i = i.updateMask(NDVI_mask)
  var m = i.get('month')
  var compositeMonth = compositesPerMonth.filter(ee.Filter.eq('month', m)).first()
  return i.select(['B2','B3','B4', 't1', 't2']).subtract(compositeMonth.select(['B2','B3','B4', 't1', 't2'])).copyProperties(i)
})
// print('diffImgs', diffImgs)


// COMPOSITE DIFFIMGS BY WEEKDAY
var weekdayList = ee.List([0,1,2,3,4,5,6])

var diffImgsPerWeekday = weekdayList.map(function(wd){
  var diffImgsWeekday = diffImgs.filter(ee.Filter.eq('weekday', wd))
  var RGB_temp = diffImgsWeekday.select('B2','B3','B4')
    .reduce(ee.Reducer.intervalMean({minPercentile:20, maxPercentile:80, maxRaw:1000}))
    .abs().reduce(ee.Reducer.max());
  var polar_temp = diffImgsWeekday.select('t1', 't2')
    .reduce(ee.Reducer.intervalMean({minPercentile:20, maxPercentile:80, maxRaw:1000}))
    .abs().reduce(ee.Reducer.max());
  return polar_temp.select('max').multiply( RGB_temp.select('max')).rename('max_pMax').set('weekday', wd);
})
diffImgsPerWeekday = ee.ImageCollection(diffImgsPerWeekday)
// print('diffImgsPerWeekday', diffImgsPerWeekday)


// DE-MEAN WEEKDAY DIFFIMGS
var diffImgMean = diffImgsPerWeekday.reduce(ee.Reducer.mean()).rename('max_pMax')
diffImgsPerWeekday = diffImgsPerWeekday.map(function(i){
  return i.subtract(diffImgMean).copyProperties(i)
})
// print('diffImgsPerWeekday', diffImgsPerWeekday)


// GET THE MAX OF ALL WEEKDAYS AND COMBINE INTO BANDS
var diffImgMax = diffImgsPerWeekday.reduce(ee.Reducer.max()).rename('max_pMax')
var diffImgFinal = diffImgsPerWeekday.merge(diffImgMax).toBands().rename(
  'weekday_0',
  'weekday_1',
  'weekday_2',
  'weekday_3',
  'weekday_4',
  'weekday_5',
  'weekday_6',
  'max_all')
// print('diffImgFinal', diffImgFinal)
// Map.addLayer(diffImgFinal, {max:20, bands:['max_all']}, 'diffImgFinal All')

//EXPORT IMAGE
var params = {
  element: diffImgFinal,
  type:'EXPORT_IMAGE',
  description: "S2diffImg_" + countryNameString + cellNumber,
  region: builtupMask.intersection(cell),
  scale: 10,
  maxPixels: 1e13,
  assetId: path+"/S2/diffImgs/cell_"+cellNumber
}

var taskId = ee.data.newTaskId(1)
ee.data.startProcessing(taskId, params);