// Dataset of markets' locations
use "temp\activity_appended_eth.dta", clear
	duplicates drop mktid, force

	su marketlat 
	local min_lat = r(min)
	local max_lat = r(max)
	su marketlon 
	local min_lon = r(min)
	local max_lon = r(max)

	keep mktid marketlat marketlon
tempfile locations
save `locations'

// Match weather data to markets // Weather exports from weather_forDraft.gee

clear
gen tmp = .
tempfile toAppend
save `toAppend', emptyok

forv i=0/9{
    di "II `i'"
		import delimited "datasets\weather\Ethiopia_rain_cell`i'of10_20250625.csv", encoding(ISO-8859-2) clear 
		
		keep if inrange(lat, `min_lat'-0.01 , `max_lat'+0.01) & inrange(lon, `min_lon'-0.01 , `max_lon'+0.01)
		//drop geo systemindex
		egen temp_cell_ID=group(lat lon)
		qui tab lat
		if r(N)>0{
			geonear temp_cell_ID lat lon  using `locations', neighbors(mktid marketlat marketlon) nearcount(1)
			rename nid mktid
			merge m:1 mktid  using `locations', nogen
			keep if km_to_nid <100 // keep only weather obs for places close to markets
			drop mktid marketlat marketlon km_to_nid temp_cell_ID		
			
			append using `toAppend'
			tempfile toAppend
			save `toAppend'
		}
}

use `toAppend', clear
egen cell_ID=group(lat lon)
drop if lon==.

tostring daterain, replace
replace daterain=daterain+"01"
gen date=date(daterain, "YMD")
format date %td
gen monthlydate =mofd(date)
format monthlydate %tm

rename total_precipitation precipitation
replace precipitation=precipitation*1000 // recorded in meters, convert to mm		
replace precipitation=400 if inrange(precipitation, 400,10000)


xtset cell_ID monthlydate
forv l=1/8{
	gen L`l'precip = L`l'.precipitation
}

drop  date
save "temp\weather_by_cell_eth.dta", replace

use "temp\weather_by_cell_eth.dta", clear
duplicates drop cell_ID, force
keep lat lon cell_ID
save "temp/weather_cells.dta", replace

use "temp\activity_appended_eth.dta", clear

geonear mktid marketlat marketlon using "temp\weather_cells.dta", neighbors(cell_ID lat lon) nearcount(1)

rename nid cell_ID
merge m:1 monthlydate cell_ID using "temp\weather_by_cell_eth.dta", keep(1 3) nogen

replace year=year(date)
replace month=month(date)
egen month_x_year = group(month year)

shp2dta using "datasets\shapefiles\ethiopia_adm2\eth_adm2.shp", data("temp/eth_adm2_data") coor("temp/eth_adm2_coor") replace


	preserve
		duplicates drop mktid, force
		geoinpoly marketlat marketlon using "temp/eth_adm2_coor"
		tempfile tmp
		save `tmp'
	restore
	capture drop _merge
	merge m:1 mktid using `tmp', keepusing(_ID) nogen
	merge m:1 _ID using "temp/eth_adm2_data", nogen keep(1 3) keepusing(shapeName)
	rename shapeName admlvl2

gen doy=doy(date)

save "temp/activityAndWeather_eth.dta", replace