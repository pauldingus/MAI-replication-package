function diffByWkd_shp(d, imgC, thres) { 
  imgC = imgC.focalMin({radius: 2, kernelType:'square', units:'pixels'})
  var img = imgC.select(imgC.bandNames().filter(ee.Filter.stringContains('item',ee.String('wd').cat(ee.Number(d).format('%.0f')))))
            .subtract(imgC.reduce(ee.Reducer.intervalMean({minPercentile:0,maxPercentile:50, maxRaw:1000})))
  return img.gt(thres).selfMask()
        .reduceToVectors({
          geometry: aoi,
          tileScale: tileScale, 
          scale: scaleOfOutcome,
        })
        .map(function (g) {
          return g.set(freqDayStr, ee.Number(d),'area', g.area(1))
        })
        .filter(ee.Filter.gt('area', areaSquare))
}

function getMktExtPerDay(d, filteredSize) { // Merge all individual market parts per day of the week
  var candExtentsPerDay = filteredSize.filter(ee.Filter.eq(freqDayStr, ee.Number(d)));
  var mergedSingleParts = ee.Feature(ee.FeatureCollection(ee.Feature(candExtentsPerDay.union(1).geometry(1).buffer(1, 1).simplify(1))
    .geometry().geometries()
    .map(function (f) { 
      return ee.Algorithms.Feature(f).set('areaBuffer', ee.Algorithms.Feature(f).area());
    }))
    .filter(ee.Filter.gt('areaBuffer', areaSquare))
    .union(1).first()).set(freqDayStr, ee.Number(d));
  var coordinateLength = mergedSingleParts.geometry().coordinates().size();
  return mergedSingleParts.set('lengthCoor', coordinateLength);
}

function fillHolesInFeature(feature) {
  // This function extracts the outermost ring of the feature's geometry, effectively removing any internal holes
  var geom = feature.geometry();
  var coords = geom.coordinates();
  //if feature is a single polygon
  var newGeom = ee.Algorithms.If(geom.type().equals('Polygon'),
    //return the first set of coordinates
    ee.Geometry.Polygon(coords.get(0)), 
    //else if feature is a multipolygon
    ee.Algorithms.If(geom.type().equals('MultiPolygon'),
      //return first set of coords for each polygon
      ee.Geometry.MultiPolygon(coords.map(function(polygon) {
        return ee.Geometry.Polygon(ee.List(polygon).get(0));
      })),
      //else return feature
      geom
    )
  )
  return ee.Feature(ee.Geometry(newGeom), feature.toDictionary());
}

function findMktsMasterFunction(thres, img, thresRange){
  thres=ee.Number.parse(thres)
  var shapes =  ee.FeatureCollection(freqList.map(function(d){return diffByWkd_shp(d, img, thres)}))
                .flatten()
  shapes = ee.FeatureCollection(freqList.map(function(d) {
    return getMktExtPerDay(d, shapes);
  })).filter(ee.Filter.gt('lengthCoor', 0));
  
  shapes=  shapes.map(function(f) {
    return fillHolesInFeature(f);
  })
  .map(function(f) {
    return f.set('thres', thres, 'strictnessRank', thresRange.indexOf(thres));
  });
  return shapes
}

function shorten_freqDayStr(input) {
  if (/weekday/i.test(input)) return "w7";
  if (/monthday/i.test(input) || /monthdayfromEnd/i.test(input)) return "w31";
  if (/weekdayEverySecond/i.test(input)) return "w14";
  if (/everyFiveDays/i.test(input)) return "w5";
  return input;
}


var locGroup= "79_Ethiopia"  //"Kenya" 
var loc= "lon38_5671lat9_2948" //"lon0_3606lat34_4016" 
var country = "Ethiopia" // "Kenya
var bucket = "p79ethiopia65" //"kenya3"
var freqListInp= 6 // 
var freqDayStr=  "weekday" // 

var aoi = ee.FeatureCollection("projects/planetupload/assets/cleanedLocs/"+country+"/"+country)
          .filter(ee.Filter.eq('mktID', ee.String(loc))).first().geometry()
Map.centerObject(aoi)
var tileScale= ee.Number(16) 
var scaleOfOutcome = ee.Number(3.1)
var areaSquare = ee.Number(50)
var freqList=ee.List.sequence(0,freqListInp,1) 

var imgAll=ee.Image('projects/'+bucket+'/assets/PS_imgs/'+locGroup+'/'+loc+'proc/diffImgApr24')

//update the diffImg to exclude the outermost pixels, where there are often ouliers due to clipping artefacts
var binary = imgAll.select('b0_50p_max_pMax_wd0').gt(0).rename('binary').unmask() //binary image indicating which pixels are unmasked
imgAll = imgAll.updateMask(
  //combine 2 masks: one masking pixels on the edge of the existing mask:
  binary.reduceNeighborhood({
      reducer: ee.Reducer.mean(),
      kernel: ee.Kernel.square(3, 'meters')
    }).gte(1).multiply(
  //one masking pixels on the edge of the image footprint:
  binary.reduceNeighborhood({
      reducer: ee.Reducer.count(),
      kernel: ee.Kernel.square(1, 'pixels')
    }).gte(9))
  )
//print(imgAll)
var stats = ee.List([ee.String('max'),ee.String('mean')])

var exportType = '50p_max_pMax' 

var bn= imgAll.bandNames().filter(ee.Filter.stringContains('item', ee.String(exportType).cat('_w')))
var img = imgAll.select(bn)

var thresRange = ee.List.sequence(2, 0.0, -0.05).map(function(i){return ee.Number(i).pow(4)}).slice(0,32)
//print('thresRange', thresRange)

var shapesAcrossRange = ee.FeatureCollection(thresRange.map(function(thres){
    return findMktsMasterFunction(thres,img,thresRange)
  })).flatten();
  
//print('shapesAcrossRange1',shapesAcrossRange)  

var collectionSize = shapesAcrossRange.size();
  
    shapesAcrossRange = ee.FeatureCollection(shapesAcrossRange.aggregate_array('strictnessRank').distinct().map(function(thres){
      var fcPerDay = shapesAcrossRange.filter(ee.Filter.eq('strictnessRank', thres)).aggregate_array('weekday').distinct()
        .map(function(d) {
          var shp = shapesAcrossRange.filter(ee.Filter.eq('strictnessRank', thres)).filter(ee.Filter.eq('weekday', d)).first()
          return shp.set('areaPerDay',ee.Feature(shp).area())
      })
      var maxSizePerWeek = ee.FeatureCollection(fcPerDay).aggregate_max('areaPerDay')
      fcPerDay = fcPerDay.map(function(f){
        return ee.Feature(f).set('shareOfMaxArea', ee.Number(ee.Feature(f).get('areaPerDay')).divide(maxSizePerWeek))
      })
      return fcPerDay
    }).flatten())
    
    //print('shapesAcrossRange2',shapesAcrossRange) 
    
    shapesAcrossRange = shapesAcrossRange.filter(ee.Filter.gt('shareOfMaxArea',0.1))
    
    //print('shapesAcrossRange3',shapesAcrossRange)  

    //label shapes as market if they are above the threshold
    var shapeswMarket = shapesAcrossRange.map(function(f){
      return f.set('area', f.area())
    })
    
    //find the day with the highest peak
    var minStrictnessRank     = shapesAcrossRange.aggregate_min('strictnessRank')
    var strictestDetection    = shapeswMarket.filter(ee.Filter.eq('strictnessRank', minStrictnessRank))
    var strictestDetectionDay = ee.Feature(strictestDetection.sort('area', false).toList(strictestDetection.size()).get(0)).get('weekday')
    var strictestDetectionDayShapes = shapeswMarket.filter(ee.Filter.eq('weekday', strictestDetectionDay))
    
    //make a feature collection of all individual geometries in the collection
    var strictestDetectionDayAll = strictestDetectionDayShapes
      .toList(strictestDetectionDayShapes.size())
      .map(function(f) {
        
        // split geometries into a list.
        var geomList = ee.Feature(f).geometry().geometries();
        
        // return each shape from each feature as a new feature
        var featureList = geomList.map(function(geom) {
          return ee.Feature(ee.Geometry(geom)).copyProperties(f);
        });
        
        return featureList;
      })
      .flatten();
    
    // filter so that only shapes intersecting with the main peak are left
    var strictestDetectionDayMain = ee.FeatureCollection(strictestDetectionDayAll).
      filterBounds(strictestDetection);
    
    //find the maximum thres from the day with the highest peak that intersects with each feature
    var finalShapes = shapeswMarket.map(function(f){
      var intersectingShapes = strictestDetectionDayMain.filterBounds(f.geometry())
      var ret = ee.Algorithms.If({
        condition: intersectingShapes.size().gt(0), 
        trueCase: f.set('maxIntersectionThres',intersectingShapes.aggregate_max('thres')), 
        falseCase: f.set('maxIntersectionThres',-99)
      })
      return ret
    }, true)
    .map(function(f){return f.set('weekdayShp', f.get('weekday'))})

var highest_day = finalShapes
                  .filter(ee.Filter.eq('strictnessRank',finalShapes.aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('highest_day',highest_day)
var second_highest_day = finalShapes
                  .filter(ee.Filter.neq('weekdayShp', highest_day))
                  .filter(ee.Filter.eq('strictnessRank',finalShapes.filter(ee.Filter.neq('weekdayShp', highest_day)).aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('second_highest_day',second_highest_day)
var third_highest_day = finalShapes
                  .filter(ee.Filter.inList('weekdayShp', [highest_day,second_highest_day]).not())
                  .filter(ee.Filter.eq('strictnessRank',finalShapes.filter(ee.Filter.inList('weekdayShp', [highest_day,second_highest_day]).not()).aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('third_highest_day',third_highest_day)

var three_highest = ee.List([highest_day, second_highest_day,third_highest_day])
//print('three_highest',three_highest)

var mktShapes = finalShapes.select(finalShapes.first().propertyNames().filter(ee.Filter.neq('item', 'weekday')))
            .filter(ee.Filter.inList('weekdayShp',three_highest))
            .filter(ee.Filter.gte('strictnessRank',8)) // throw out the very center of the market areas
            .filter(ee.Filter.lte('strictnessRank',30)) // 
            .map(function(f){
              var fgeo=f.geometry()
              return f.set('share_of_AOI', fgeo.area().divide(aoi.area(1)))
                      .set('polygons_in_shape', fgeo.coordinates().size())
                      .set('polygon_type', fgeo.type())
                      .set('subStrictnessRank',100) // necessary so that this property is included in exports
            })
            .filter(ee.Filter.lte('share_of_AOI',.3)) // throw out shapes that are covering a lot of the AOI, usually complex ones
            .filter(ee.Filter.or(ee.Filter.eq('polygon_type','Polygon'), ee.Filter.and(ee.Filter.eq('polygon_type','MultiPolygon'),ee.Filter.lt('polygons_in_shape',10))))
            
print('mktShapes',
  mktShapes.sort('strictnessRank'),
  mktShapes.sort('share_of_AOI'),
  mktShapes.aggregate_array('share_of_AOI'),
  mktShapes.aggregate_array('area').sort(),
  mktShapes.aggregate_array('weekdayShp').sort().distinct(),
  mktShapes.aggregate_array('strictnessRank').sort().distinct()
  )

var avlbl_weekdays = mktShapes.aggregate_array('weekdayShp').distinct()

var possibleShapes = mktShapes.sort('strictnessRank',false).merge(ee.FeatureCollection(avlbl_weekdays.map(function(d){ // identify all weekdays
  var relevant_shapes_per_day = mktShapes.filter(ee.Filter.eq('weekdayShp',d))
  var ranksPerDay= relevant_shapes_per_day.aggregate_array('strictnessRank').distinct() // identify all ranks per weekday
  var slicesPerLevel = ranksPerDay.map(function(s){
    var smallerLevels = ranksPerDay.filter(ee.Filter.lt('item',s))
    var slicesPerSmallerLevel =  smallerLevels.map(function(ss){
      var shape1 = relevant_shapes_per_day.filter(ee.Filter.eq('strictnessRank',s)).first().geometry(1) 
      var shape2 = relevant_shapes_per_day.filter(ee.Filter.eq('strictnessRank',ss)).first().geometry(1) 
      var area1 = shape1.area(1)
      var area2 = shape2.area(1)
      var share_area = area2.divide(area1)
      var slice = ee.Feature(ee.FeatureCollection(shape1.difference(shape2,3).simplify(3.1).geometries()
                  .map(function(geo){
                    return ee.Algorithms.Feature(geo).set('geotype', ee.Algorithms.Feature(geo).geometry().type())
                  }))
                  .filter(ee.Filter.inList('geotype', ['Polygon','MultiPolygon'])).union(1).first())
                  .copyProperties(relevant_shapes_per_day.filter(ee.Filter.eq('strictnessRank',s)).first())
                  .set('ring_share_area',share_area)
                  .set('subStrictnessRank',ss) // needs to be last so it overwrites previously copied values
      return slice
    })
    return slicesPerSmallerLevel
  }).flatten()
  return slicesPerLevel
}).flatten())
.filter(ee.Filter.lt('ring_share_area',0.8))) // throw out rings that are very thin

    var params={ // EXPORT FOR SHAPEFILE
      element: possibleShapes,
      type: 'EXPORT_FEATURES',
      description: "shp_"+loc+"_"+exportType,
      outputPrefix: locGroup+'/shapes/shp_MpM6_'+locGroup+loc,
      fileFormat: 'SHP',
      outputBucket: 'exports-mai2023'
    }
    
    var taskId = ee.data.newTaskId(1);
    ee.data.startProcessing(taskId, params);
    
    var params={ // EXPORT FOR ASSET FOR FURTHER PROCESSING
      element: possibleShapes,
      type: 'EXPORT_FEATURES',
      description: "as2_"+loc+"_"+exportType,
      assetId: "projects/"+bucket+"/assets/PS_imgs/"+locGroup+"/"+loc+"proc/shp_MpM6_"+locGroup+loc,
    }
    
    var taskIdAsset = ee.data.newTaskId(1);
    ee.data.startProcessing(taskIdAsset, params);
    

