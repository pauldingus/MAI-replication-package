// SEASONALITY
use "temp/activityAndWeather_eth.dta", replace
keep if admlvl2=="Central" & admlvl1 == "Tigray" & inrange(date, td(01oct2017), td(30jun2020))
egen mktCode= group(mktid)
local year 2018
gen monthToMerge = _n if _n<37
gen yearToMerge = 2018 if inrange(monthToMerge,1,12)
replace yearToMerge = 2019 if inrange(monthToMerge,13,24)
replace yearToMerge = 2020 if inrange(monthToMerge,25,30)
replace monthToMerge = monthToMerge-12 if yearToMerge==2019
replace monthToMerge = monthToMerge-24 if yearToMerge==2020

preserve 
	//keep if year==`year'
	duplicates drop month year mktid, force
	collapse (mean) precipitation, by(month year)
	rename month monthToMerge
	rename year yearToMerge
	rename precipitation mean_precip_by_month
	tempfile tmp
	save `tmp'
restore 

merge m:1 monthToMerge yearToMerge using `tmp', nogen

gen monthmid = mdy(monthToMerge, 15, yearToMerge)
format monthmid %td

gen growing = inlist(monthToMerge,6,7,8,9)
gen harvest = inlist(monthToMerge,10,11,12)
gen lower_grow = 340
gen upper_grow = 290
gen lower_harv = 290
gen upper_harv = 240

local lab1 = td(15jan2018)
local lab2 = td(15jul2018)
local lab3 = td(15jan2019)
local lab4 = td(15jul2019)
local lab5 = td(15jan2020)
local lab6 = td(15jul2020)

local labharv = td(01oct2019) 
local labgrow = td(25sep2019) 

local left =td(15feb2018)


tw  (rbar lower_grow upper_grow monthmid if growing==1, barw(32) fc("$color2*0.8") lw(0)) ///
	(rbar lower_grow upper_grow monthmid if harvest==1, barw(32) fc("$color3*0.8") lw(0)) ///
	(bar mean_precip_by_month monthmid,  barw(15) color("gs6%50") lw(0))  ///
	, ytitle("Rainfall (mm)" ) xtitle("") ///
	xlabel(`lab1' "1/2018" `lab2' "7/18" `lab3' "1/19" `lab4' "7/19" `lab5' "1/20" `lab6' "7/20" ) name(rain, replace) graphregion(color(white) margin(none)) title(" ", pos(10)) ylabel(0(200)200, angle(0)) legend(off) ///
	text(410 `labgrow'  "Growing ", color("$color2") placement(w)) /// 
	text(410 `labharv'  "Harvest", color("$color3") placement(e))  fysize(20) ///
	title("B", color(white)) plotregion(lstyle(none))
local bw=20
local degree=0
	
tw  (lpolyci activity_harmonized_2018 date if mktday==1 , bw(`bw') degree(`degree') lcolor("$color1") fcolor("$color1%25") alw(0) lw(*2)) /// 
	(lpolyci activity_harmonized_2018 date if mktday==0 , bw(`bw') degree(`degree') lcolor("$color1%50") fcolor("$color1%25")  alw(0) lw(*2)) ///
	,  name(seasonal, replace) xtitle("") ///
	ytitle("Market activity index") legend(off) ///
	xlabel(`lab1' " " `lab2' " " `lab3' " " `lab4' " " `lab5' " " `lab6' " " , ticks labsize(0)) xscale(range(`lab1'(100)`lab6')) ///
	ylabel(0 50 100, angle(0)) ///
	title("{bf:B}", pos(10) color(black)) ///
	text(20 `left'  "Non-market" "days", color("$color1*0.5") placement(c) ) ///
	text(140 `left' "Market days", color("$color1") placement(c))  graphregion(color(white) margin(none))  plotregion(lstyle(none))
	
graph combine seasonal rain, name(seasonality, replace)  rows(2) iscale(*1.325) ///
		graphregion(margin(-2 +0 +0 +0))