use "datasets\LSMS\2015\ETH_HouseholdGeovars_y3.dta", clear
keep ea_id2 lat_dd_mod lon_dd_mod
duplicates drop ea_id2, force
tempfile tmp
save `tmp'

use "datasets\LSMS\2015\sect04_com_w3.dta", clear

keep ea_id2 cs4q14 cs4q15 sa1q01 

merge 1:1 ea_id2 using `tmp', nogen

decode sa1q01, gen(sa1q01_str)
drop if inlist(sa1q01_str, "Addis Ababa")
rename lat_dd_mod lat_mod
rename lon_dd_mod lon_mod
rename ea_id2 ea_id
gen round=2015
tempfile round2015
save `round2015'

use "datasets\LSMS\2018\ETH_HouseholdGeovariables_Y4.dta", clear
keep ea_id lat_mod lon_mod
duplicates drop ea_id, force
tempfile tmp
save `tmp'

use "datasets\LSMS\2018\sect04_com_w4.dta", clear

keep ea_id cs4q14 cs4q15 saq01 saq14

merge 1:1 ea_id using `tmp', nogen

decode saq01, gen(sa1q01_str)
drop if inlist(sa1q01_str, "14. ADDIS ABABA")
gen round=2018

tempfile round2018
save `round2018'

use "datasets\LSMS\2021\eth_householdgeovariables_y5.dta", clear
gen ea_id = substr(household_id, 1, strlen(household_id) - 3)
keep ea_id lat_dd_mod lon_dd_mod
duplicates drop ea_id, force
tempfile tmp
save `tmp'

use "datasets\LSMS\2021\sect04_com_w5.dta", clear

keep ea_id cs4q14 cs4q15 saq01 saq14

merge 1:1 ea_id using `tmp', nogen keep(3)

decode saq01, gen(sa1q01_str)
drop if inlist(sa1q01_str, "14. ADDIS ABABA")
gen round=2021
rename lat_dd_mod lat_mod
rename lon_dd_mod lon_mod

tempfile round2021
save `round2021'

use `round2015', clear
append using `round2018'
append using `round2021'
egen group =group(ea_id round)

preserve
	shp2dta using "datasets\shapefiles/eth_detectedMarkets.shp", replace data(data_eth) coor(coor_eth)
	use data_eth, replace
	keep mktID marketLat marketLon
	tempfile tmp
	save `tmp'
restore

geonear group lat_mod lon_mod  using `tmp', neighbors(mktID marketLat marketLon) nearcount(1) ellipsoid

replace cs4q15 = 0 if cs4q14 ==1
gen log_km = log(km_to_nid)
replace log_km = log(1) if log_km<log(1)

/*
hist log_km if cs4q14 == 1 | cs4q15<5 , bin(100) fraction ///
    xlabel(`=log(0.5)' "`=0.5'" `=log(1)' "`=1'" `=log(5)' "`=5'" `=log(10)' "`=10'" `=log(50)' "`=50'" `=log(100)' "`=100'" ) xtitle("Kilometers to nearest detected market") ytitle("Share of communities with local market") by(round) name(hist, replace)

hist log_km if cs4q14 == 1 | cs4q15<5, bin(100) fraction ///
    xlabel(`=log(0.5)' "`=0.5'" `=log(1)' "`=1'" `=log(5)' "`=5'" `=log(10)' "`=10'" `=log(50)' "`=50'" `=log(100)' "`=100'" ) xtitle("Kilometers to nearest detected market") ytitle("Share of communities with local market")  name(histall, replace)
*/

gen detected_market_in_community = .
	replace detected_market_in_community = 1 if cs4q14==1 | cs4q15<=5
	replace detected_market_in_community = 0 if cs4q14==2 & cs4q15>5
	
ksmirnov km_to_nid, by(detected_market_in_community)
if r(p)<0.001{
	local pvalue "<0.001"
}
else{
	local pvalue = round(r(p),0.001)
}

distplot log_km if km_to_nid<300, over(detected_market_in_community) ///
	legend(off) ///
	xscale(range(`=log(1)' `=log(300)')) ///
	xlabel(`=log(1)' "1" `=log(5)' "5" `=log(10)' "10" `=log(50)' "50" `=log(100)' "100" `=log(300)' "300", labsize(small)) ///
	ylabel(,labsize(small)) ///
	xtitle("Straight-line distance from assigned community centroid" "to closest detected market (km)", size(small)) ///
	ytitle("Cumulative share of 2015/18/21 LSMS enumeration areas", size(small)) ///
	text(.6 `=log(100)' "p-value on K-S test of equality" "of distributions: `pvalue'", size(small)) color("$color2" "$color3") ///
	text(.7 `=log(1.8)' "Stated: market within" "5km from community", size(small) color("$color3") placement(e)) ///
	text(.3 `=log(100)' "Stated: no market within" "5km from community", size(small) color("$color2") placement(w))
graph display, xsize(16) ysize(16)
graph export "graphs/figure_lsms.png", replace height(2000)

