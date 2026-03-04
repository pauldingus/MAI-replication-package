// FIGURE 5
// Load ADM-2 boundaries with some minor adjacent zones merged
spshape2dta "datasets\shapefiles\redrawnZonesDissolved_20211129.shp", replace

use redrawnZonesDissolved_20211129_shp, clear
    gen _EMBEDDED = 0
    replace _EMBEDDED = 1 if _ID == 24
save redrawnZonesDissolved_20211129_shp, replace

// Prepare conflict data
import delimited "datasets\conflict\2012-07-01-2025-07-01-Ethiopia.csv", clear 

drop if event_type == "Strategic developments"
replace admin1 = "Other" if !inlist(admin1, "Tigray", "Oromia", "Amhara")

gen event_date_pre = date(event_date, "DMY")
format event_date_pre %td
drop event_date
rename event_date_pre event_date

keep if inrange(event_date, td(01jan2018), td(31dec2024))

gen month=month(event_date)

gen countEvent = 1
preserve
	collapse (sum) countEvent, by(month year admin1)
	reshape wide countEvent, i(month year) j(admin1) string
	gen countTigrayANDAmhara = countEventTigray+ countEventAmhara
	gen countTigrayANDAmharaANDOromia = countTigrayANDAmhara+ countEventOromia
	gen countALL = countTigrayANDAmhara+ countEventOromia + countEventOther
	gen day=15
	gen date=mdy(month, day, year)
	save "temp/conflict_adm1.dta", replace
restore
preserve
	capture drop _ID
	gen quarter=quarter(event_date)
	collapse (sum) countEvent, by( year quarter admin1)
	rename admin1 admlvl1
	save "temp/conflict_adm1_quarter.dta", replace
restore

// Lower panel of Fig. 5A 
use "temp/conflict_adm1.dta", clear

tw (bar countEventTigray date, barw(25) col("$color1") lw(0)) ///
   (rbar countTigrayANDAmhara countEventTigray date , barw(25) col("$color2") lw(0)) ///
   (rbar countTigrayANDAmharaANDOromia countTigrayANDAmhara date, barw(25) col("$color3") lw(0)) ///
   (rbar countALL countTigrayANDAmharaANDOromia  date, barw(25) col("gs10") lw(0)) ///
   , graphregion(color(white) margin(0 0 0 0)) ///
	 xlabel(`=td(15jan2018)' "Jan 2018" `=td(15jan2019)' "Jan '19" `=td(15jan2020)' "Jan '20" `=td(15jan2021)' "Jan '21" `=td(15jan2022)' "Jan '22" `=td(15jan2023)' "Jan '23" `=td(15jan2024)'"Jan '24" `=td(15jan2025)' "Jan 2025", angle(45) labsize(huge)) ///
	 xtick(`=td(15jan2018)' `=td(15jan2019)' `=td(15jan2020)' `=td(15jan2021)' `=td(15jan2022)' `=td(15jan2023)') ///
	 name(bars, replace) ytitle("# monthly" "conflict events" , size(huge)) ///
	 legend(off) xtitle("") ylabel(0(200)200, labsize(huge)) plotregion(lstyle(none)) nodraw

use "temp/activityAndWeather_eth.dta", replace
keep if mktday==1
keep if inrange(date, td(01jan2018), td(31dec2024))
replace admlvl1 = "Other" if !inlist(admlvl1, "Tigray", "Oromia", "Amhara")

encode admlvl2, gen(admlvl2_code)

// generate deseasonalized measure
reg activity_harmonized_2019 i.month#i.admlvl2_code if inrange(date, td(01jan2018), td(28feb2020)) & inrange(activity_harmonized_2019, -50, 300)
predict seasonal_mktAct_2018 if activity_harmonized_2019 != . & inrange(activity_harmonized_2019, -50, 300), xb
gen deseas_mktAct_2018 = activity_harmonized_2019 - seasonal_mktAct_2018

// assign markets to adm-2 zones
capture drop _ID
 geoinpoly  marketlat marketlon  using redrawnZonesDissolved_20211129_shp
 
merge m:1 _ID using redrawnZonesDissolved_20211129, keep(1 3) nogen
rename _ID _ID_adm2

// Upper panel of Fig. 5A 
tw  (scatter deseas_mktAct_2018 date if inrange(deseas_mktAct_2018,-10,10), mc(white%0)) ///
	(lpoly deseas_mktAct_2018  date if admlvl1=="Tigray", color("$color1") bw(40) lw(*2)) ///
	(lpoly deseas_mktAct_2018 date if admlvl1=="Amhara", color("$color2") bw(40) lw(*2)) ///
	(lpoly deseas_mktAct_2018 date if admlvl1=="Oromia", color("$color3") bw(40) lw(*2)) ///
	(lpoly deseas_mktAct_2018 date if admlvl1=="Other", color("gs10") bw(40)) ///
	, legend(off) name(lines, replace) ytitle("Deseasonalized" "market activity",size(huge)) yline(0, lp(dash) lc(gs8)) xlabel(none) ///
	xtick(`=td(15jan2018)' `=td(15jan2019)' `=td(15jan2020)' `=td(15jan2021)' `=td(15jan2022)' `=td(15jan2023)' `=td(15jan2024)' `=td(15jan2025)' , grid) ylabel(, labsize(huge)) ///
	graphregion(margin(0 0 0 0))  plotregion(lstyle(none)) nodraw

graph combine lines bars,  row(2) name(comb, replace) title("{bf:a}", ring(1) pos(10) size(huge)) graphregion(margin(0 0 0 1))

graph display, xsize(8) ysize(6) 

preserve
	gen quarter=quarter(date)
	collapse (mean) deseas_mktAct_2018, by(year quarter admlvl1)
	save "temp/act_by_quarter_adm1.dta", replace
restore 

gen mkts_per_adm = .
levelsof _ID_adm2, local(adms)
foreach adm of local adms{
	qui tab mktid if _ID_adm2 ==`adm'
	replace mkts_per_adm = r(r) if _ID_adm2 ==`adm'
}

collapse (mean) deseas_mktAct_2018 (first) admlvl1 mkts_per_adm, by(year _ID_adm2)
save "temp/act_by_year_adm2.dta", replace // save for Figure 5C

// Figure 5B
clear
set obs 4
gen admlvl1 = ""
	replace admlvl1 ="Tigray" in 1
	replace admlvl1 ="Amhara" in 2
	replace admlvl1 ="Oromia" in 3
	replace admlvl1 ="Other" in 4
	expand 5
	bysort admlvl1: gen year=_n
	replace year=year+2019
	expand 4
	bysort admlvl1 year: gen quarter=_n

	
	merge 1:1 admlvl1 year quarter using  "temp/conflict_adm1_quarter.dta", keep(1 3) nogen
	replace countEvent = 0 if countEvent==.
	
	local tgt_count countEvent
	
	merge 1:1  admlvl1 year quarter using "temp/act_by_quarter_adm1.dta", keep(1 3)

	keep if year>2020 | (year==2020 & quarter==4)
	
	local msyms "O" "D" "T" "S" "X"
	
	reg deseas_mktAct_2018 countEvent, robust
	local b= round(_b[countEvent], 0.001)
	local se = round(_se[countEvent], 0.001)
	if abs(`b' / `se') >2.64{
		local stars "***"
	}
	
	global colorLA gs10
	global colorTigray $color1
	global colorAmhara $color2
	global colorOromia $color3
	foreach adm1 in LA Tigray Amhara Oromia  {
		forv year=2020/2024{
			local y =`y'+1
			local yy=`y'-1
			local msym : word `y' of "`msyms'"
			local add (scatter deseas_mktAct_2018 countEvent if year==`year' & admlvl1=="`adm1'", msymbol(`msym') mc("${color`adm1'}"))
			local tw `tw' `add' 
			forvalues q=1/4{
				su deseas_mktAct_2018 if year==`year' & admlvl1=="`adm1'" &  quarter==`q'
				local ycoor=r(mean)
				su countEvent if year==`year' & admlvl1=="`adm1'" &  quarter==`q'
				local xcoor=r(mean)
				if "`xcoor'"!="." {
					if (`year'==2020 &  `xcoor'>300) | (`year'==2021 &  `xcoor'>300) | (`year'==2024 &  `ycoor'<-20) {
						local xxcoor= `xcoor' +3
						local text `text' text(`ycoor' `xxcoor' "202 ", color("gs6")  placement(w))
					}
					local text `text' text(`ycoor' `xcoor' "`yy'", color("${color`adm1'}") placement(c))
				}
			}
		}
		local add (lfit deseas_mktAct_2018 countEvent if admlvl1=="`adm1'" & year>2020, lp(dash) lc("${color`adm1'}"))
		// local tw `tw' `add' 
		local y=0
		local tw2  `tw2'  (scatter deseas_mktAct_2018 countEvent if admlvl1=="`adm1'", mcolor("${color`adm1'}") msymbol(O))
	}

tw `tw2' (lfit deseas_mktAct_2018 countEvent, lp(dash) lc(gs6)), legend(off) ytitle("Mean quarterly market""activity, deseasonalized", size(medlarge)) xtitle("Conflict events per quarter", size(medlarge)) name(comb1, replace) title("{bf:b}", ring(1) pos(10) size(large)) fxsize(45) /// `text' 
graphregion(margin(0 2 0 0)) xlabel(,labsize(medlarge)) ylabel(,labsize(medlarge)) plotregion(lstyle(none)) /// yline(0, lp(dash) lc(gs8))
   text(-22 350 "`b'`stars'" , placement(c) color(gs6)) /// 
   text(-26 350  "(`se')", placement(c) color(gs6)) 
   // text( 40 400 "`b_Tigray'`starsTigray' (`se_Tigray')", placement(w) color($color1)) ///
   // text( 34 400 "`b_Amhara'`starsAmhara' (`se_Amhara')", placement(w) color($color2)) ///
   // text( 28 400 "`b_Oromia'`starsOromia' (`se_Oromia')", placement(w) color($color3)) 
   
graph display, xsize(6) ysize(6)

/// FIGURE FOR APPENDIX ///
	local tw2 
	reg deseas_mktAct_2018 countEvent, robust
	local b= round(_b[countEvent], 0.001)
	local se = round(_se[countEvent], 0.001)
	if abs(`b' / `se') >2.64{
		local stars "***"
	}
	
	foreach adm1 in Tigray Amhara Oromia{
		reg deseas_mktAct_2018 countEvent if admlvl1=="`adm1'", robust
		local b_`adm1'= round(_b[countEvent], 0.001)
		local se_`adm1' = round(_se[countEvent], 0.001)
		if abs(`b_`adm1'' / `se_`adm1'') >2.64{
			local stars`adm1' "***"
		}
	}
	
	foreach adm1 in LA Tigray Amhara Oromia  {
		forv year=2020/2024{
			local y =`y'+1
			local yy=`y'-1
			local msym : word `y' of "`msyms'"
			local add (scatter deseas_mktAct_2018 countEvent if year==`year' & admlvl1=="`adm1'", msymbol(`msym') mc("${color`adm1'}"))
			local tw `tw' `add' 
			forvalues q=1/4{
				su deseas_mktAct_2018 if year==`year' & admlvl1=="`adm1'" &  quarter==`q'
				local ycoor=r(mean)
				su countEvent if year==`year' & admlvl1=="`adm1'" &  quarter==`q'
				local xcoor=r(mean)
				if "`xcoor'"!="." {
					if (`year'==2020 &  `xcoor'>300) | (`year'==2021 &  `xcoor'>300) | (`year'==2024 &  `ycoor'<-20) {
						local xxcoor= `xcoor' +3
						local text `text' text(`ycoor' `xxcoor' "202 ", color("gs6")  placement(w))
					}
					local text `text' text(`ycoor' `xcoor' "`yy'", color("${color`adm1'}") placement(c))
				}
			}
		}
		local add (lfit deseas_mktAct_2018 countEvent if admlvl1=="`adm1'" & year>2020, lp(dash) lc("${color`adm1'}"))
		// local tw `tw' `add' 
		local y=0
		local tw2  `tw2'  (scatter deseas_mktAct_2018 countEvent if admlvl1=="`adm1'", mcolor("${color`adm1'}") msymbol(O))
	}

tw `tw2' (lfit deseas_mktAct_2018 countEvent, lp(dash) lc(gs6)) ///
		 (lfit deseas_mktAct_2018 countEvent if admlvl1=="Tigray", lp(dash) lc("$color1")) ///
		 (lfit deseas_mktAct_2018 countEvent if admlvl1=="Amhara", lp(dash) lc("$color2")) ///
		 (lfit deseas_mktAct_2018 countEvent if admlvl1=="Oromia", lp(dash) lc("$color3")) ///
, legend(off) ytitle("Mean quarterly market activity, deseasonalized", size(medlarge)) xtitle("Conflict events per quarter", size(medlarge))  /// 
graphregion(margin(0 2 0 0)) xlabel(,labsize(medlarge)) ylabel(,labsize(medlarge)) plotregion(lstyle(none)) /// yline(0, lp(dash) lc(gs8))
   text(22 400 "All: `b'`stars' (`se')" , placement(w) color(gs6)) /// 
   text(40 400 "Tigray: `b_Tigray'`starsTigray' (`se_Tigray')", placement(w) color($color1)) ///
   text(34 400 "Amhara: `b_Amhara'`starsAmhara' (`se_Amhara')", placement(w) color($color2)) ///
   text(28 400 "Oromia: `b_Oromia'`starsOromia' (`se_Oromia')", placement(w) color($color3)) 
   
graph display, xsize(6) ysize(6)
graph export "graphs/figure_Slines.png", replace height(2000)
/// END, back to Fig. 5


// Figure 5C
use "temp/act_by_year_adm2.dta", clear
gen _ID =_ID_adm2
merge m:1 _ID using redrawnZonesDissolved_20211129, keepusing(_ID)
replace _ID_adm2= _ID if _ID_adm2==.

levelsof _ID_adm2, local(adm2s) clean
levelsof year, local(years) clean
foreach adm2 of local adm2s {
    foreach yr of local years {

        quietly count if _ID_adm2 == `adm2' & year == `yr'

        if r(N) == 0 {
            set obs `=_N + 1'
            replace _ID_adm2 = `adm2' in L
            replace year      = `yr'   in L
        }
    }
}



replace deseas_mktAct_2018=. if mkts_per_adm<5

local steps = 5
colorpalette red "252 211 202", ipolate(`steps') power(1)  nograph
local colors `r(p)' "199 218 197"
su deseas_mktAct_2018
local max=ceil(r(max))
local min=floor(r(min))
forv year=2020/2024{
	if `year'== 2024{
		local margin=0
		local legend `" ring(0) pos(1) size(medlarge) symxsize(*2) order(7 ">0" 6 "<0" 5 "<-10" 4 "<-20" 3 "<-30" 2 "<-40" 1 "<5 detected" - "markets") title("Market activity" "rel. to 2018-19", size(medlarge)) region(color(white%80)) "'
		local fxsize 
		local labsize medlarge
	}
	else{
		local margin = 0
		local legend off
		local fxsize 
		local labsize vlarge
	}
	if `year'==2020{
		local subtitle `""{bf:c}" , ring(0) pos(11) size(7)"'
	}
	else{
		local subtitle
	}
	preserve
		keep if year==`year' | year==.
		duplicates drop _ID_adm2, force
		spmap deseas_mktAct_2018 using redrawnZonesDissolved_20211129_shp.dta, id(_ID_adm2) name(map`year', replace) ndfcolor(white) clmethod(custom) clbreaks(`min' -40 -30 -20 -10 0   `max') fcolor("`colors'") title("`year'", ring(0) pos(5) size(`labsize')) graphregion(color(white) margin(0 `margin' 0 0)) legend(`legend') fxsize(`fxsize')  subtitle(`subtitle') 
	restore
	
}
graph combine map2020  map2021 map2022  map2023, row(2) name(mapscomb, replace)  imargin(zero) graphregion(margin(0 0 0 0)) nodraw
graph combine mapscomb map2024, graphregion(margin(0 0 0 +2)) name(lowerRow, replace) 

graph display, ysize(4) xsize(9)

// legend map
spshape2dta "datasets\shapefiles\Eth_Adm1.shp", replace 

use Eth_Adm1_shp, clear
	gen _ID_switch = _ID
	replace _ID_switch = 7 if _ID==8
	replace _ID_switch = 8 if _ID==7
save Eth_Adm1_shp, replace

use Eth_Adm1, clear

gen c=0
	replace c = 1 if ADM1_NAME=="Tigray"
	replace c = 2 if ADM1_NAME=="Amhara"
	replace c = 3 if ADM1_NAME=="Hareri"
	replace c = 4 if ADM1_NAME=="Oromia"
gen _ID_switch = _ID
	replace _ID_switch = 7 if _ID==8
	replace _ID_switch = 8 if _ID==7

spmap c using Eth_Adm1_shp.dta , id(_ID_switch) fcolor(white "$color1" "$color2"  "$color3") clbreaks(-1 0 1 2 3) clmethod(unique) split mfcolor(white) legend(off) name(legendMap, replace) fxsize(35) /// 
text(14.25 43 "Tigray", color("$color1") placement(e) bcolor(white%80) size(vlarge) box) ///
text(12 43 "Amhara", color("$color2") placement(e) bcolor(white%80) size(vlarge)  box) ///
text(9.2 43 "Oromia", color("$color3") placement(e) bcolor(white%80)  size(vlarge) box) graphregion(color(white) margin(0 0 0 0)) nodraw

graph combine comb legendMap, name(comb2, replace) graphregion(margin(0 0 0 0))  fysize(50) // fxsize(30) nodraw 
graph combine comb2 comb1, name(comb3, replace) graphregion(margin(0 0 0 0))

graph combine comb3 lowerRow, row(2) graphregion(margin(0 0 0 0))  name(figure5, replace)
graph display, ysize(12) xsize(16)

graph export "graphs/Fig_6.png", replace height(2000)
graph export "graphs/Fig_6.pdf", replace height(7.0866142)




