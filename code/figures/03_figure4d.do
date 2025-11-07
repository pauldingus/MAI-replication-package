// SHOCKS
import delimited "datasets\weather\Ethiopia_rain_byadm2_20250625.csv", encoding(ISO-8859-2) clear 
drop geo shapeid shapeiso systemindex shapegroup shapetype
rename shapename admlvl2
rename mean precipitation
replace precipitation = precipitation*1000
replace precipitation=400 if inrange(precipitation, 400,10000)
tostring daterain, replace
replace daterain=daterain+"01"
gen date=date(daterain, "YMD")
format date %td
gen monthly_date =mofd(date)
format monthly_date %tm
gen month=month(date)
gen year=year(date)
keep if inlist(month, 6,7,8)
collapse (sum) precipitation, by(year admlvl2)

egen mean = mean(precipitation), by(admlvl2)
egen sd = sd(precipitation), by(admlvl2)
gen gs_rain_shock = (precipitation-mean)/sd
gen gs_rain_shock_sq = gs_rain_shock^2

save "temp/growshocks_eth_adm2.dta", replace

use "temp/activityAndWeather_eth.dta", replace
local act_norm_year 2018
* find for each market the month in which it is usually busiest
keep if mktday==1 & !inlist(admlvl1, "Afar", "Somali") & inlist(month, 10,11,12)

capture drop _merge
gen monthly_date =mofd(date)

gen markets_per_adm2 = .
levelsof admlvl2, local(adms)
foreach adm of local adms{
	qui tab mktid if admlvl2=="`adm'"
	replace markets_per_adm2 = r(r)  if admlvl2=="`adm'"
}

collapse  (median) activity_harmonized_* (mean) marketlat marketlon markets_per_adm2 (first) admlvl1, by(admlvl2 year )

merge 1:1 year admlvl2 using "temp/growshocks_eth_adm2.dta", keep(3) nogen

encode admlvl2, gen(admlvl2_code)
areg  activity_harmonized_2018 gs_rain_shock gs_rain_shock_sq if year!=2020, absorb(year)

forv y = 2018/2018{
	binscatter activity_harmonized_`y' gs_rain_shock if !inlist(year, `y') , linetype(lfit)  name(g`y'none, replace) nodraw  note(no FE) medians

	areg activity_harmonized_`y' gs_rain_shock if !inlist(year, `y'), absorb(admlvl2) 
	local beta = round(_b[gs_rain_shock],0.01)
	local se = round(_se[gs_rain_shock],0.01) 
	binscatter activity_harmonized_`y' gs_rain_shock if !inlist(year, `y') , linetype(lfit)  absorb(admlvl2) name(g`y'adm, replace) nodraw note("`beta', `se'***") medians

	binscatter activity_harmonized_`y' gs_rain_shock if !inlist(year, `y') , linetype(lfit)  absorb(year) name(g`y'year, replace) nodraw note(yearFE) medians

	binscatter activity_harmonized_`y' gs_rain_shock if !inlist(year, `y') , linetype(lfit)  absorb(admlvl2) name(g`y'yearadm2, replace) nodraw controls(i.year) note(year & adm FE) medians
	
}
//graph combine g2018none g2018adm g2018year g2018yearadm2


reg activity_harmonized_2018 gs_rain_shock , absorb(admlvl2) vce(cluster admlvl2)
local beta = round(_b[gs_rain_shock],0.01)
local se = round(_se[gs_rain_shock],0.01) 

binscatter activity_harmonized_2018 gs_rain_shock   , linetype(lfit)   name(g`y'adm, replace) medians mc("42 157 143") lc("233 196 106") nq(15)  absorb(admlvl2) ///
 savedata(temp/tmp.csv) replace nodraw
 
 
preserve
	clear
	insheet using temp/tmp.csv.csv

	tw (scatter activity_harmonized_2018 gs_rain_shock, mcolor("$color1") lcolor("$color1")) ///
	   (lfit activity_harmonized_2018 gs_rain_shock, lcolor("$color3") lw(*2)) ///
	   ,  legend(off) name(shocks, replace) title("{bf:D}", pos(10) color(black)) ///
	ylabel(100(10)120, angle(0)) yscale(range(100(10)120)) ///
	   text(116.5 1.8 "`beta'***" , placement(c) color("$color3")) ///
	   text(115 1.8  "(`se')", placement(c) color("$color3")) ///
	   xtitle("Rainfall shocks during previous growing season") ytitle("Market acitvity during harvest season") ///
		graphregion(margin(-2 0 0 0)) plotregion(lstyle(none))
	graph display, ysize(10) xsize(10) 
 restore
 
