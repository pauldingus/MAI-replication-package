// Table S2
use "temp/activityAndWeather_eth.dta", replace
keep if mktday==1
keep if inrange(date, td(01jan2018), td(31dec2024))

preserve
	keep if inrange(activity_harmonized_2019, -50, 300) & inrange(date, td(01jan2018), td(28feb2020))
	
	collapse (median) activity_harmonized_2019, by(admlvl2 month admlvl1)
	
	bysort  admlvl1 admlvl2  (activity_harmonized_2019): gen rank_active_months =_n
	
	egen maxRank = max(rank_active_months), by(admlvl1 admlvl2)
	
	gen diffFromMaxRank = abs(rank_active_months-maxRank)+1
	forv m=1/3{
		gen leastBusyMonths_`m' = .
		gen mostBusyMonths_`m' = .
	}
	egen admunit = group(admlvl1 admlvl2)
	qui levelsof admunit, local(admunits)
	foreach unit of local admunits{
		forv m=1/3{
			qui levelsof month if rank_active_months == `m' & admunit==`unit', local(month) 
			qui replace leastBusyMonths_`m' = `month' if admunit==`unit'
		}	
		forv m=1/3{
			qui levelsof month if diffFromMaxRank == `m' & admunit==`unit', local(month) 
			qui replace mostBusyMonths_`m' = `month' if admunit==`unit'
		}	
	}
	duplicates drop admunit, force
	keep admlvl2 admlvl1 leastBusyMonths_* mostBusyMonths_*
	tempfile leastBusyMonths
	save `leastBusyMonths'
restore

merge m:1 admlvl2 admlvl1 using `leastBusyMonths', nogen

gen  lean_season_vs_normal = .
	replace lean_season_vs_normal = 1 if (inlist(month, leastBusyMonths_1,leastBusyMonths_2 ,leastBusyMonths_3))
	replace lean_season_vs_normal = 0 if (!inlist(month, leastBusyMonths_1,leastBusyMonths_2 ,leastBusyMonths_3, mostBusyMonths_1,mostBusyMonths_2 ,mostBusyMonths_3))
label var lean_season_vs_normal "Lean season reduction"

*-----------------------------------------------
* Regression: All regions (Amhara, Oromia, Tigray)
*-----------------------------------------------
areg activity_harmonized_2019 lean_season_vs_normal ///
    if inrange(date, td(01jan2018), td(28feb2020)) & ///
       inrange(activity_harmonized_2019, -50, 300), ///
    absorb(mktid) vce(cluster month) 

outreg2 using lean_season_results, replace ///
    ctitle("All") bdec(3) tdec(3) label word

*-----------------------------------------------
* Province-specific regressions
*-----------------------------------------------
foreach adm in Tigray Amhara Oromia {
    areg activity_harmonized_2019 lean_season_vs_normal ///
        if inrange(date, td(01jan2018), td(28feb2020)) & ///
           inrange(activity_harmonized_2019, -50, 300) & ///
           admlvl1=="`adm'", ///
        absorb(mktid) vce(cluster month)

    outreg2 using lean_season_results, append ///
        ctitle("`adm'") bdec(3) tdec(3) label word

}
