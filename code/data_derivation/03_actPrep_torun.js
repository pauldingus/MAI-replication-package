///////////////// 
/// FUNCTIONS /// 
///////////////// 
function shorten_freqDayStr(input) { 
  if (/weekday/i.test(input)) return "w7"; 
  if (/monthday/i.test(input) || /monthdayfromEnd/i.test(input)) return "w31";
  if (/weekdayEverySecond/i.test(input)) return "w14";
  if (/everyFiveDays/i.test(input)) return "w5";
  return input;
}

// Function to control for the 'instrument' property being missing for some images
var instruments =ee.Dictionary({
  "_1_" : "PS2", // images that have _1_ in position -8 are PS2
  "3B" : "PS2", // images that have 3B in position -3 are PS2; all others are Ps2.SD or PSB.SD
})

function instrument_null(image){
  //var key1 = ee.String(image.get('instrument'))
  var key2 = ee.String(image.get('imageID')).slice(-2)
  var key3 = ee.String(image.get('imageID')).slice(-8,-5) 
  return image.set('instrument',instruments.get(key2,instruments.get(key3,'PSB.SD'))) 
}

// for histogram matching
var lookup = function(sourceHist, targetHist) {
  // Split the histograms by column and normalize the counts.
  var sourceValues = sourceHist.slice(1, 0, 1).project([0])
  var sourceCounts = sourceHist.slice(1, 1, 2).project([0])
  sourceCounts = sourceCounts.divide(sourceCounts.get([-1]))

  var targetValues = targetHist.slice(1, 0, 1).project([0])
  var targetCounts = targetHist.slice(1, 1, 2).project([0])
  targetCounts = targetCounts.divide(targetCounts.get([-1]))

  // Find first position in target where targetCount >= srcCount[i], for each i.
  var lookup = sourceCounts.toList().map(function(n) {
    var index = targetCounts.gte(n).argmax()
    return targetValues.get(index)
  })
  return {x: sourceValues.toList(), y: lookup}
}

// Make the histogram of sourceImg match targetImg.
var histogramMatch = function(sourceImg, targetImg) {
  var geom = sourceImg.geometry();
  var args = {
    reducer: ee.Reducer.autoHistogram({maxBuckets: 256, cumulative: true}), 
    geometry: geom, // don't replace with AOI because want image-wide correction, not just around marketplace
    scale: 3.1,
    maxPixels: 65536 * 4 - 1,
    bestEffort: true,
    tileScale:16
  }
  
  // Only use pixels in target that have a value in source (inside the footprint and unmasked).
  var source = sourceImg.reduceRegion(args)
  var target = targetImg.updateMask(sourceImg.select('B1').mask()).reduceRegion(args)

  return ee.Image.cat(
    sourceImg.select(['B1']).interpolate(lookup(source.getArray('B1'), target.getArray('B1'))),
    sourceImg.select(['B2']).interpolate(lookup(source.getArray('B2'), target.getArray('B2'))),
    sourceImg.select(['B3']).interpolate(lookup(source.getArray('B3'), target.getArray('B3')))
  )
}

// Function to find number of permissible images within 42/84 days. Permissible means from the same generation of sensors
function find_permissible_images(image, collection){
  var image_date = ee.Date(image.get('date_as_date'));
  var permissible = permissible_sensors.get(image.get('instrument'))
  
  var collection_84 = collection
                      .filterDate(image_date.advance(-84,'day'), image_date.advance(84,'day'))
                      .filter(ee.Filter.inList('instrument', permissible))
  var collection_42 = collection_84.filterDate(image_date.advance(-42,'day'), image_date.advance(42,'day'))
  
  var permissible_images_42 = collection_42.size()
  var permissible_images_84 = collection_84.size().subtract(permissible_images_42)

  return image.set('perm_imgs_42',permissible_images_42,
                   'perm_imgs_84',permissible_images_84)

}

// Function to create weighted composite
function createComposite2(d, dateIndex, instrument, collection) {
  var date = ee.Date(ee.String(d).slice(0,10))
  //create property containing the images' distance to the reference image in terms of ordered position
  var icRelativeIndexed = ee.ImageCollection(collection)
      .filterDate(date.advance(-42,'day'), date.advance(42,'day'))
      .map(function(img){
        var relativePosition = ee.Number(img.get('date_order')).subtract(ee.Number(dateIndex)).abs();
        return ee.Image(img).set('dateIndexDistance', relativePosition, 'dayDate', ee.Date(img.get('date_as_date')))
    }, true)
  
  // For each weekday in the list, filter the image collection to contain just that weekday, and create a weighted sample surrounding our image.
  var wkdSamples = wkdList.map(function(weekday){
    var icFilteredForWeekday = icRelativeIndexed.filter(ee.Filter.eq('weekday', weekday)).sort('dateIndexDistance');
    var within1 = ee.ImageCollection(icFilteredForWeekday.toList(2));
    var within2 = ee.ImageCollection(icFilteredForWeekday.toList(4));
    var within3 = ee.ImageCollection(icFilteredForWeekday.toList(6));
    return within1.merge(within2).merge(within3)
  })
  
  // Merge all collections in the list
  var mergedSamples = wkdSamples.iterate(function(current, prev) {
    return ee.ImageCollection(prev).merge(current);
  }, ee.ImageCollection([])); // Start with an empty image collection
  
  mergedSamples = ee.ImageCollection(mergedSamples)
  
  var sampledIDs = mergedSamples.aggregate_array('system:id')
  
  // Retrieve the total counts for all pixels in the collection
  var samplesCount = ee.ImageCollection(mergedSamples).count();
    
  // Create another sample and mask if the count is above the threshold
  var extraSamples = extendComposite(d, dateIndex, instrument, icIndexed, samplesCount.lt(30), sampledIDs);
    
  // Merge original and extra samples
  mergedSamples = ee.ImageCollection(mergedSamples).merge(extraSamples)
    
  // Merge into the final composite
  return ee.ImageCollection(mergedSamples)
      .reduce(ee.Reducer.intervalMean({minPercentile:40, maxPercentile:60, maxRaw:1000}))
      .set('Date', d, 'size',mergedSamples.size(), 'instrument',instrument);// 'meanPixels', meanPixels,
}

// Function to create extended composite samples
function extendComposite(d, dateIndex, instrument, collection, mask, alreadySampled) {
  var date = ee.Date(ee.String(d).slice(0,10))

  //create property containing the images' distance to the reference image in terms of ordered position
  var icRelativeIndexed = collection.filterDate(date.advance(-84,'day'), date.advance(84,'day'))
    .filter(ee.Filter.inList('system:id', alreadySampled).not())
    .filter(ee.Filter.gt('shareOfAOI', 0.5))
    .map(function(img){
      var relativePosition = ee.Number(img.get('date_order')).subtract(ee.Number(dateIndex)).abs();
    return ee.Image(img).set('dateIndexDistance', relativePosition, 'dayDate',ee.Date(ee.String(img.get('Date')).slice(0,10)))
  }, true)
  
  // For each weekday in the list, filter the image collection to contain just that weekday, and create a weighted sample surrounding our image.
  var wkdSamples = wkdList.map(function(weekday){
    var icFilteredForWeekday = icRelativeIndexed.filter(ee.Filter.eq('weekday', weekday)).sort('dateIndexDistance');
    return ee.ImageCollection(icFilteredForWeekday.toList(6)); 
  })
  
  // Merge all collections in the list
  var extraSamples = wkdSamples.iterate(function(current, prev) {
    return ee.ImageCollection(prev).merge(current);
  }, ee.ImageCollection([])); // Start with an empty image collection
  
  // Mask based on which pixels are below the count threshold
  extraSamples = ee.ImageCollection(extraSamples)
    .map(function(i){
      return i.updateMask(mask)
    })
  return extraSamples
}

function diffToComposite(image, compositeCollection) { // calculate for each image the difference to the composite
  // Get the image from the composite collection that matches the image's date and sensor
  var composite = compositeCollection
        .filter(ee.Filter.eq('Date', image.get('Date')))
        .filter(ee.Filter.eq('instrument', image.get('instrument'))).get(0)
  var compositeSize = ee.Image(composite).get('size')
  var wkdy = image.get('weekday')
  var ret = ee.Image(image).subtract(ee.Image(composite))
    .set('weekday',wkdy, 'size', compositeSize)
    .set('system:time_start',image.get('system:time_start'))
    .copyProperties(image)
  return ee.Image(ret)
}

// Function to add the polar coordinate bands to each image
function polarCoor_Conversion(image){
  var temp_r = image.select('B3');
  var temp_g = image.select('B2');
  var temp_b = image.select('B1');
  var temp_rho = temp_r.pow(2).add(temp_g.pow(2)).add(temp_b.pow(2)).pow(0.5).rename('rho');
  var temp_theta1 = temp_r.pow(2).add(temp_g.pow(2)).pow(0.5).divide(temp_b).atan().rename('t1');
  var temp_theta2 = temp_g.divide(temp_r).atan().rename('t2');
  return image.addBands(temp_rho).addBands(temp_theta1).addBands(temp_theta2);
}


//////////////////   
/// PARAMETERS ///
////////////////// 


var locGroup= "79_Ethiopia" // "170_Mozambique" 
var bucket = "p79ethiopia65"//"p170mozambique4" 
var country = "Ethiopia" //"Mozambique" 
var loc= "lon38_5671lat9_2948" //"lon33_9351lat-20_3856" 

var freqListInp= 6 // 
var freqDayStr=  "weekday" 
var freqList=ee.List.sequence(0,freqListInp,1) 

var permissible_sensors = ee.Dictionary({
  "PS2": ee.List([ee.String('PS2')]),
  "PS2.SD": ee.List([ee.String('PS2.SD'),ee.String('PSB.SD')]),
  "PSB.SD": ee.List([ee.String('PS2.SD'),ee.String('PSB.SD')]),
});

//////////////////
/// INPUTS ///////
//////////////////
var linkDateWeekday = ee.FeatureCollection("projects/planetupload/assets/linkDateWeekday")
var aoi_all = ee.FeatureCollection("projects/planetupload/assets/cleanedLocs/"+country+"/"+country)
          .filter(ee.Filter.eq('mktID', ee.String(loc))).first().geometry()

var mktShapes = ee.FeatureCollection("projects/"+bucket+"/assets/PS_imgs/"+locGroup+"/"+loc+"proc/shp_MpM6_"+locGroup+loc)
                .filter(ee.Filter.eq('subStrictnessRank',100))
                .map(function(f){return f.set('weekdayShp', f.get('weekday'))})

var highest_day = mktShapes
                  .filter(ee.Filter.eq('strictnessRank',mktShapes.aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('highest_day',highest_day)
var second_highest_day = mktShapes
                  .filter(ee.Filter.neq('weekdayShp', highest_day))
                  .filter(ee.Filter.eq('strictnessRank',mktShapes.filter(ee.Filter.neq('weekdayShp', highest_day)).aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('second_highest_day',second_highest_day)
var third_highest_day = mktShapes
                  .filter(ee.Filter.inList('weekdayShp', [highest_day,second_highest_day]).not())
                  .filter(ee.Filter.eq('strictnessRank',mktShapes.filter(ee.Filter.inList('weekdayShp', [highest_day,second_highest_day]).not()).aggregate_min('strictnessRank')))
                  .aggregate_min('weekdayShp')
//print('third_highest_day',third_highest_day)

var three_highest = ee.List([highest_day, second_highest_day,third_highest_day])
//print('three_highest',three_highest)

mktShapes = mktShapes.select(mktShapes.first().propertyNames().filter(ee.Filter.neq('item', 'weekday')))
            .filter(ee.Filter.inList('weekdayShp',three_highest))
            .filter(ee.Filter.gte('strictnessRank',8)) // throw out the very center of the market areas
            .map(function(f){
              return f.set('share_of_AOI', f.geometry().area().divide(aoi_all.area()))
                      .set('polygons_in_shape', f.geometry().coordinates().size())
                      .set('polygon_type', f.geometry().type())
            })
            .filter(ee.Filter.lte('share_of_AOI',.3)) // throw out shapes that are covering a lot of the AOI, usually complex ones
            .filter(ee.Filter.or(ee.Filter.eq('polygon_type','Polygon'), ee.Filter.and(ee.Filter.eq('polygon_type','MultiPolygon'),ee.Filter.lt('polygons_in_shape',10) )))

//Map.centerObject(mktShapes)

var aoi = mktShapes.union(1).geometry()//.convexHull()
var mktDays=mktShapes
            .filter(ee.Filter.lte('strictnessRank',ee.Number(mktShapes.aggregate_min('strictnessRank')).add(2)))
            .aggregate_array(ee.String(freqDayStr).cat('Shp')).distinct()

//get list of weekdays other than market days:
var wkdList = freqList.filter(ee.Filter.inList('item', mktDays).not());

var tileScale= aoi.area(1).divide(500000).ceil().min(16).max(4)

var ic = ee.ImageCollection(ee.ImageCollection("projects/"+bucket+"/assets/PS_imgs/"+locGroup+ "/" + loc)
        .filterBounds(aoi.convexHull()) // keep only images that cover the detected market area
        .map(function(i){
          var harmonized = ee.String(i.get('system:index')).index('harmonized').gte(0)
          return i.set('hasQ8',i.bandNames().contains('Q8'))
                  .set('harmonized', harmonized)
        })
        .filter(ee.Filter.eq('hasQ8',true))     // important
        .filter(ee.Filter.eq('harmonized',1))   // important. Not all images harmonized because of faulty delivery
        .map(function(i){
          var mask=i.select('Q1').eq(0)         // identify pixels that are marked as shadow or other
                    .add(i.select('Q8').neq(0)) // identify pixels that are marked as not clear
                    .eq(0)                      // identify pixels where neither of those are true. These are valid pixels.
                    .unmask({sameFootprint:false})
          var time = ee.Date(i.get('system:time_start')).format('HH:mm:ss')
          var timeDec = ee.Number.parse(time.slice(0,2)).add(ee.Number.parse(time.slice(3,5)).divide(60)).add(ee.Number.parse(time.slice(6,8)).divide(3600))
          var date =  ee.Date(i.get('system:time_start')).format('YYYY-MM-dd')
          var meanValues = i.select(['B3','B2','B1']).addBands(mask.eq(1).rename('mask'))
                            .reduceRegion({
                              reducer: ee.Reducer.mean().combine(ee.Reducer.stdDev(),'',true), 
                              geometry: aoi, 
                              scale: 10,
                              tileScale: 16
                            });
          
          return i.updateMask(mask)  
                  .set('shareOfAOI', meanValues.get('mask_mean'))
                  .set('Date',date.cat('a'))
                  .set('date_as_date', ee.Date(date))
                  .set("imageID", ee.String(i.get('system:index')).slice(0,23))
                  .set('timeDec', timeDec, 'sd', meanValues)
                  .set('B1_mean', meanValues.get('B1_mean'), 'B2_mean', meanValues.get('B2_mean'), 'B3_mean', meanValues.get('B3_mean'))
                  .set('B1_sd', meanValues.get('B1_stdDev'), 'B2_sd', meanValues.get('B2_stdDev'), 'B3_sd', meanValues.get('B3_stdDev'))
                  .set('month',ee.Date(date).get('month'))
        },true)
        .map(instrument_null)                   // set instrument information for images where it is missing
        .select('B1','B2','B3')                 // keep only RGB bands
        .filter(ee.Filter.gt('shareOfAOI',0.1)) // keep only images that cover substantial share of AOI, post-masking
        .filterMetadata('shareOfAOI', 'not_equals', null) // ideally would do this at the download stage
        .filter(ee.Filter.gt('B1_sd',0)).filter(ee.Filter.gt('B2_sd',0)).filter(ee.Filter.gt('B3_sd',0)) // Some images have constant band values; faulty
        ).distinct('imageID')

var ic_old = ic.filter(ee.Filter.eq('instrument','PS2'))
var B1_mean_stats_old = ic_old.aggregate_stats('B1_mean');
var B2_mean_stats_old = ic_old.aggregate_stats('B2_mean');
var B3_mean_stats_old = ic_old.aggregate_stats('B3_mean');

var ic_new = ic.filter(ee.Filter.inList('instrument', ['PS2.SD','PSB.SD']))
var B1_mean_stats_new = ic_new.aggregate_stats('B1_mean');
var B2_mean_stats_new = ic_new.aggregate_stats('B2_mean');
var B3_mean_stats_new = ic_new.aggregate_stats('B3_mean');

print('ic',ic)
ic_old =ic_old       // keep only those images where band values are normal; excludes faullty captures
     .filter(ee.Filter.lt('B1_mean', (ee.Number(B1_mean_stats_old.get('mean')).add(ee.Number(B1_mean_stats_old.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B1_mean', (ee.Number(B1_mean_stats_old.get('mean')).subtract(ee.Number(B1_mean_stats_old.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.lt('B2_mean', (ee.Number(B2_mean_stats_old.get('mean')).add(ee.Number(B2_mean_stats_old.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B2_mean', (ee.Number(B2_mean_stats_old.get('mean')).subtract(ee.Number(B2_mean_stats_old.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.lt('B3_mean', (ee.Number(B3_mean_stats_old.get('mean')).add(ee.Number(B3_mean_stats_old.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B3_mean', (ee.Number(B3_mean_stats_old.get('mean')).subtract(ee.Number(B3_mean_stats_old.get('sample_sd')).multiply(2)))))

ic_new =ic_new       // keep only those images where band values are normal; excludes faullty captures
     .filter(ee.Filter.lt('B1_mean', (ee.Number(B1_mean_stats_new.get('mean')).add(ee.Number(B1_mean_stats_new.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B1_mean', (ee.Number(B1_mean_stats_new.get('mean')).subtract(ee.Number(B1_mean_stats_new.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.lt('B2_mean', (ee.Number(B2_mean_stats_new.get('mean')).add(ee.Number(B2_mean_stats_new.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B2_mean', (ee.Number(B2_mean_stats_new.get('mean')).subtract(ee.Number(B2_mean_stats_new.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.lt('B3_mean', (ee.Number(B3_mean_stats_new.get('mean')).add(ee.Number(B3_mean_stats_new.get('sample_sd')).multiply(2)))))
     .filter(ee.Filter.gt('B3_mean', (ee.Number(B3_mean_stats_new.get('mean')).subtract(ee.Number(B3_mean_stats_new.get('sample_sd')).multiply(2)))))

ic =ee.ImageCollection(ic_new.merge(ic_old))
var medianTime = ee.Number(ic.reduceColumns(ee.Reducer.median(), ['timeDec']).get('median'))

var target_images=ee.ImageCollection(ee.List.sequence(1,12,1) // define target images for histogram matching; median composites of second generation sensors, within 2020/21, covering most of the AOI
  .map(function(month){
    var elig = ic_new
                 .filterDate('2020-01-01','2021-12-31')
                 .filter(ee.Filter.gt('shareOfAOI',.75))
                 .filter(ee.Filter.gt('timeDec', medianTime.subtract(.5)))
                .filter(ee.Filter.lt('timeDec', medianTime.add(.5)))
                .map(function(i){
                  var diff2020 = ee.Date(i.get('system:time_start')).difference(ee.Date.fromYMD(2020, month, 15) ,'day').abs();
                  var diff2021 = ee.Date(i.get('system:time_start')).difference(ee.Date.fromYMD(2021, month, 15) ,'day').abs();
                  var minDiff = diff2020.min(diff2021);
                  return i.set('diff_to_month', minDiff);
                }).sort('diff_to_month',true).limit(50);
    return ee.ImageCollection(elig).median().set('month',month);
  }));
// print('target images',target_images)

var ic_hm = ic.map(function(image){ // perform histogram matching on each image, matched to a composite from the relevant month
  var ref_img = target_images.filter(ee.Filter.eq('month', image.get('month'))).first();
  return histogramMatch(image, ref_img).copyProperties(image).set('system:time_start', image.get('system:time_start'))
})
//print('ic_hm',ic_hm)

ic = ic_hm
       .map(polarCoor_Conversion)           // apply polar conversion
       .select('B1','B2','B3','t1','t2')    // keep only relevant bands
       .map(function(image){                // clip images to AOI (post matching, so areas for histograms are large enough)
         return image.clipToCollection(ee.FeatureCollection(ee.Feature(aoi.convexHull()))) 
       })

//////////////////
/// PROCESSING ///
//////////////////
 
// Assign to each image its weekday based on the date
var prepLink = linkDateWeekday.map(function(f) {
  return f.set('Date',ee.String(f.get('dateStr')).cat('a'))
})

var icWwkd = ee.Join.inner('primary','secondary','match') // assign to each image its weekday
            .apply(ic, prepLink, ee.Filter.equals({leftField: 'Date', rightField: 'Date'}));

var imgsWwkds= icWwkd.map(function(f){
    var image=ee.Feature(f).get('primary')
    var freqDay = ee.Feature(ee.Feature(f).get('secondary')).get(freqDayStr)
    return ee.Image(image).set(ee.String(freqDayStr), ee.Number(freqDay))
}).sort('system:time_start');
//print('imgsWwkds', imgsWwkds)

// Add a sequential number to each image in the collection
var icList = imgsWwkds.toList(imgsWwkds.size())
var idList = ee.List.sequence(0, imgsWwkds.size().subtract(1))

var icIndexed = ee.ImageCollection(idList.map(function(i){
  var index = idList.get(i)
  var f = icList.get(i)
  return ee.Image(f).set('date_order', index).copyProperties(f)
}))
//print('icIndexed', icIndexed, icIndexed.aggregate_histogram('instrument'))

var icIndexed_forComposites=icIndexed       // exclude images that are not high quality
      .filter(ee.Filter.gt('timeDec', medianTime.subtract(.5)))
      .filter(ee.Filter.lt('timeDec', medianTime.add(.5)))
      .filter(ee.Filter.gt('shareOfAOI',0.5))

var icIndexed_forDiffs = icIndexed // exclude images for which no good composites can be constructed
      .map(function(image){
        return find_permissible_images(image,icIndexed_forComposites)
      })
      .filter(ee.Filter.gte('perm_imgs_42',6))
      .filter(ee.Filter.gte('perm_imgs_84',6))

//create composites for each unique day in the image:
var compositeCollection = icIndexed_forDiffs.aggregate_array('Date').distinct().map(function(date){ //
  return icIndexed_forDiffs.filter(ee.Filter.eq('Date', date)).aggregate_array('instrument').distinct().map(function (instrument) {
    var example_image = ee.Image(icIndexed_forDiffs.filter(ee.Filter.eq('Date', date)).filter(ee.Filter.eq('instrument', instrument)).first());
    var collection =  icIndexed_forComposites
      .filter(ee.Filter.inList('instrument', permissible_sensors.get(instrument))) // keep only images that are from the right sensor given the image's sensor
      .filterDate(ee.Date(example_image.get('date_as_date')).advance(-84,'day'), ee.Date(example_image.get('date_as_date')).advance(84,'day'))
    return createComposite2(example_image.get('Date'), example_image.get('date_order'), instrument, collection);
  }).flatten()
}).flatten()
//print('compositeCollection', compositeCollection)

//Create an image collection containing differences to the composite for each image
var diffQ_r1 = icIndexed_forDiffs
              .map(function(image){
                return diffToComposite(image, compositeCollection)}, true)
              .filter(ee.Filter.gt('size',10))
//print('diffQ_r1',diffQ_r1)

var diffsByDay = ee.ImageCollection(diffQ_r1.map(function(i){
  var RGB_temp = i.select('B1','B2','B3')
      .abs().reduce(ee.Reducer.max())
  var polar_temp = i.select('t1', 't2')
      .abs().reduce(ee.Reducer.max())
  var RGB_max_polar_max = polar_temp.select('max').multiply(RGB_temp.select('max')).rename('maxpMax');

  return ee.Image(RGB_max_polar_max)
  .copyProperties(i)

}))

var toBands = diffsByDay.select(['maxpMax']).toBands() 
var sorted = toBands.bandNames().sort()

var bandNames = sorted.map(function(b){
  return ee.String('b').cat(ee.String(b).slice(2,25)).cat(ee.String(b).slice(ee.String(b).index("_maxpMax")))
})

toBands = toBands.select(sorted, bandNames)
print('toBands maxpMax',toBands)

var params = {
  element: toBands,
  type:'EXPORT_IMAGE',
  description: 'actPrep'+'_'+loc,
  region: aoi,
  scale: 3.1,
  assetId: "projects/"+bucket+"/assets/PS_imgs/"+locGroup+"/"+loc+"proc/"+ 'expJune06maxpMax'+'_'+loc
}

var taskId = ee.data.newTaskId(1)
ee.data.startProcessing(taskId, params);  
