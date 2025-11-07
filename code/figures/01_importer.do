
global endOfData = td(31dec2024)
local d1 = td(01nov2020)
local d2 = td(01may2021)
global refRange2 `d1',`d2'

import delimited "datasets/activity_raw/df_ETH_20250623_batch0.csv", clear
tempfile tmp
save `tmp'

forv b=1/16{
	import delimited "datasets/activity_raw/df_ETH_20250623_batch`b'.csv", clear
	append using `tmp'
	tempfile tmp
	save `tmp'
}

gen datePre = date(date, "YMD")

drop date
rename datePre date
format date %td
keep if inlist(mktday,0,1)

rename location mktid


gen month=month(date)
gen year=year(date)

keep if inrange(diff_to_median_time,-0.1,.75) | diff_to_median_time==.

keep if clear_percent > 90 
keep if date > td(01jul2017)

// Linking series across the generation divide
foreach thres in 1M 2_5M 5M{
	shp2dta using "datasets\equal_pop_regions/subregions_maxpop_`thres'.shp", coor(temp/coor_`thres') data(temp/data_`thres') replace
	 preserve
		duplicates drop mktid, force
		keep mktid marketlat marketlon
		geoinpoly marketlat marketlon using temp/coor_`thres' 
		rename _ID id_equalpop_`thres'
		sepscatter marketlat marketlon, sep(id_equalpop_`thres') name(g_equalpop_`thres', replace)
		tempfile tmp
		save `tmp'
	 restore
	 merge m:1 mktid using `tmp', keepusing(id_equalpop_`thres') nogen
}

preserve
	duplicates drop mktid, force
	keep marketlat marketlon admlvl1 mktid
	gen cluster = .
	local N_pre=0
	levelsof admlvl1, local(adms)
	foreach adm of local adms{
		di "`adm'"
		qui tab mktid if admlvl1 == "`adm'"
		di r(N)
		local N =max(1,round(r(N)/50,1))
		di "`N'"
		cluster kmeans marketlat marketlon if admlvl1 == "`adm'", k(`N') gen(clusters_adm)
		replace cluster = clusters_adm + `N_pre' if admlvl1 == "`adm'"
		su cluster
		local N_pre = r(max)
		drop clusters_adm
	}
	
	sepscatter marketlat marketlon, sep(cluster) name(clusters, replace)
	tempfile clusters
	save `clusters'
restore

merge m:1 mktid using `clusters'

gen monthlydate = ym(year, month)

preserve
	gen PS2 = (instrument=="PS2")
	collapse (mean) PS2, by(monthlydate)
	levelsof monthlydate, local(ms) clean
	foreach m of local ms{
		qui su PS2 if monthlydate==`m'
		local share =r(mean)
		if `share'<0.8 & "`notyet'"==""{
			local notyet "no"
			local m1 =`m'
		}
		if `share'<0.2 & "`already'"==""{
			local already "yes"
			local m2 =`m'-1
		}	
	}
	
	di `m1', `m2'
	tw (scatter PS2 monthlydate), yline(0.2) yline(0.8) xline(`m1') xline(`m2')
restore

global m1 =`m1'
global m2 =`m2'

global dm1 =  dofm(`m1')
global dm2 =  dofm(`m2')


// Loop over the target years from 2018 to 2023. Activity is defined for 2018-2020 for the old generation, and 2021-2023 for the new generation


forv tgtyear = 2018/2023 {

	// Create a new variable to hold the harmonized activity measure for this year
	// (this is just a copy for the values that are already defined for the target series; will be filled in later for the undefined values after harmonization)
	gen activity_harmonized_`tgtyear' = activity_measure_norm_`tgtyear' // only where series exists

	if `tgtyear' < 2021 {
		gen activity_to_be_rescaled = activity_measure_norm_2021
	}
	else{
		gen activity_to_be_rescaled = activity_measure_norm_2020
	}
	foreach cluster_var in id_equalpop_2_5M { //cluster id_equalpop_1M  id_equalpop_5M
		
		// Compute the mean and standard deviation of the current year's activity measure
		// Only include observations within a valid date range ($m1 to $m2) and sensible activity range (-50 to 300)
		egen mean_own_pre = mean(activity_measure_norm_`tgtyear') if ///
			inrange(monthlydate, $m1, $m2) & inrange(activity_measure_norm_`tgtyear', -50, 300), ///
			by(`cluster_var' mktday)
		
		egen sd_own_pre = sd(activity_measure_norm_`tgtyear') if ///
			inrange(monthlydate, $m1, $m2) & inrange(activity_measure_norm_`tgtyear', -50, 300), ///
			by(`cluster_var' mktday)

		// Compute the mean and standard deviation of the target year's activity measure
		egen mean_tgt_pre = mean(activity_to_be_rescaled) if ///
			inrange(monthlydate, $m1, $m2) & inrange(activity_to_be_rescaled, -50, 300), ///
			by(`cluster_var' mktday)
		
		egen sd_tgt_pre = sd(activity_to_be_rescaled) if ///
			inrange(monthlydate, $m1, $m2) & inrange(activity_to_be_rescaled, -50, 300), ///
			by(`cluster_var' mktday)

		foreach var in mean_tgt sd_tgt mean_own sd_own {
			egen `var' = max(`var'_pre), by(cluster mktday)
		}

		// Harmonize the activity data using the z-score formula:
		//     new_value = (old_value - mean_own) * (sd_tgt / sd_own) + mean_tgt
		// This rescales each year's data to match the reference year's distribution
		replace activity_harmonized_`tgtyear' = ///
				(activity_to_be_rescaled - mean_tgt) * (sd_own / sd_tgt) + mean_own if activity_measure_norm_`tgtyear'==.
		
		replace activity_harmonized_`tgtyear' = . if !inrange(activity_harmonized_`tgtyear',-50,300)

		
		drop mean_tgt* sd_tgt* mean_own* sd_own*  activity_to_be_rescaled
		
		tw  (scatter activity_harmonized_`tgtyear' date if inrange(activity_harmonized_`tgtyear',100,105), msize(0) mc(white)) ///
			(lpolyci activity_harmonized_`tgtyear' date if mktday==1  & inrange(activity_harmonized_`tgtyear', -50,300) & admlvl1=="Tigray", color(green) fcolor(green%50) alw(0) ) ///
			(lpolyci activity_harmonized_`tgtyear' date if mktday==1  & inrange(activity_harmonized_`tgtyear', -50,300) & admlvl1=="Amhara", color(blue) fcolor(blue%50) alw(0) ) ///
			(lpolyci activity_harmonized_`tgtyear' date if mktday==1  & inrange(activity_harmonized_`tgtyear', -50,300) & admlvl1=="Oromia", color(red) fcolor(red%50) alw(0) ) ///
			(lpolyci activity_harmonized_`tgtyear' date if mktday==1  & inrange(activity_harmonized_`tgtyear', -50,300) & admlvl1=="SNNPR", color(orange) fcolor(orange%50) alw(0) ) ///
			, legend(order(3 "tigray" 5 "amhara" 7 "Oromia" 9 "SNNPR") row(1) pos(6)) title("`tgtyear'") name(g`tgtyear'`cluster_var', replace) nodraw
	
	
	}
}
graph combine g2018id_equalpop_2_5M g2019id_equalpop_2_5M g2020id_equalpop_2_5M g2021id_equalpop_2_5M g2022id_equalpop_2_5M g2022id_equalpop_2_5M, xcommon ycommon name(`cluster_var', replace)


global range "inrange(date, td(01jul2017), $endOfData)"
keep if $range

gen instrument_gen = ""
	replace instrument_gen = "old" if inlist(instrument, "PS2")
	replace instrument_gen = "new" if inlist(instrument, "PS2.SD", "PSB.SD")

capture drop _merge
save "temp/activity_appended_eth.dta", replace	