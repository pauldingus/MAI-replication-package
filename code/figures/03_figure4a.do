
use "temp/activityAndWeather_eth.dta", replace

egen mktCode= group(mktid)
	
local target =td(05feb2018)
local range=10
local drange=`range'*2+1

keep if  inrange(date, `target'-`range', `target'+`range')
keep if activity_harmonized_2018 !=. & instrument=="PS2"
egen countMktDayObs =sum(mktday) , by(mktid)
tab mktid if countMktDayObs == 4

keep if mktid =="lon38_5671lat9_2948" & inlist(mktday,0,1)

su date 
local min=r(min) //-1
local max=r(max) //+1
format date  %tdDay_Mon_DD
generate date_text2 = string(date, "%tdDay_DD")
	replace date_text2 = subinstr(date_text2, "Wed", "We",.)
	replace date_text2 = subinstr(date_text2, "Thu", "Th",.)
	replace date_text2 = subinstr(date_text2, "Fri", "Fr",.)
	replace date_text2 = subinstr(date_text2, "Sat", "Sa",.)
	replace date_text2 = subinstr(date_text2, "Sun", "Su",.)
	replace date_text2 = subinstr(date_text2, "Mon", "Mo",.)
	replace date_text2 = subinstr(date_text2, "Tue", "Tu",.)

duplicates drop mktid date, force

	
forv d=`min'/`max'{
    di "`d'", "`min'", "`max'"
    levelsof date_text2 if date==`d' & mktday==1, local(dd) clean
	if "`dd'"==""{
		//label def dates `d' " ", add
	}

	else{
		label def dates `d' "`dd'", add
		local labels `labels' `d'
	}
	*levelsof date_text2 if date==`d' & mktday==0, local(dd) clean
	*if "`dd'"=="" & !inlist(`d', td(13oct2022), td(16oct2022), td(23oct2018), td(30oct2018)){
	*label def mdates `d' " ", add
	*}

	else if inlist(`d', td(27jan2018),  td(15feb2018) ){
	    //local dd = substr("`dd'",1,2)
		label def dates `d' "`dd'", add
		local mlabels `mlabels' `d'
	}
}
di "`labels', `mlabels'"
label val date dates
gen mdate=date
label val mdate mdates


su date if mktday==1
su date if mktday==0
local left=r(max)-4
tw (bar activity_harmonized_2018 date if mktday==0 , lw(0) color("$color1*0.5") barw(0.7)) ///
   (bar activity_harmonized_2018 date if mktday==1 , lw(0) color("$color1") barw(0.7)) ///
   , graphregion(color(white) margin(medsmall)) legend(off) ///
   xtitle("Valid imagery acquisitions, Jan - Feb '18") xlabel(`labels',valuelabel labcolor("$color1") angle(45)) xmlabel(`mlabels',valuelabel angle(90) tlength(*2) labsize(medsmall)) xscale(range(`min'(1)`max')) ///
   yscale(range(-10(25)100)) ylabel(0 50 100, angle(0)) ytitle("Market activity index" ) title("{bf:A}", pos(10) color(black)) /// 
    name(panela, replace) ///
	text(-7 `left'  "Non-market days", color("$color1*0.5") placement(c) ) ///
	text(95 `left' "Market days", color("$color1") placement(c) ) ///
		graphregion(margin(-2 +0 +0 +0)) plotregion(lstyle(none))
	