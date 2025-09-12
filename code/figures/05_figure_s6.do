// POWER FIGURE
use "temp/activityAndWeather_eth.dta", replace
* find for each market the month in which it is usually busiest
capture drop _merge
gen quarter=quarter(date)

keep if admlvl1=="Amhara"

preserve // dataframe to save results in
	clear 
	set obs 0
	gen samplesize = .
	gen alpha = ""
	gen power = ""
	gen delta = .
	gen interval=""
	gen obs_per_interval =.
	tempfile tostore
	save  `tostore'
restore

local maxSampleSize 80 // Maximum number of marketplaces in sample
local control_mean 0 // not important
local draws 500 // # of draws to construct estimates of standard error and number of market-day observations by market-sample

forv sam = 0(1)6{ // larger sets of marketplaces
	local samplesize = 2^`sam'
	di "sample size: `samplesize'"
	matrix mean_se = J(`draws', 7, .)
	// construct summary statistics for `draws' random draws of marketplaces
	forv draw = 1/`draws'{ 
		sort mktid
		preserve // select `samplesize' marketplaces
			qui bysort mktid: keep if _n == 1
			qui sample `samplesize', count
			sort mktid
			tempfile tmp
			qui save `tmp'
		restore
		
		qui merge m:1 mktid using `tmp'
		// tab mktid if _merge==3
		// run regression to calculate standard deviation of activity measure by month
		matrix diag_m = J(12,2,.)
		forv m=1/12{
			capture noisily {
				qui reg activity_harmonized_2019 if mktday == 1 & _merge==3 & year==2019 & month==`m'
				matrix diag_m[`m',1] = _se[_cons]
				matrix diag_m[`m',2] = e(N)
			}
		}
		// number of observations by month for a market sample of size `samplesize'
		svmat diag_m
		qui su diag_m1 if diag_m1 > 0
		local mean_se = r(mean)
		qui su diag_m2 if diag_m1 > 0
		local N = r(mean)
		// collect estimates of standard error and # of obs
		matrix mean_se[`draw',1]= `draw'
		matrix mean_se[`draw',2]= `mean_se'
		matrix mean_se[`draw',3]= `N'
		
		// run regression to calculate standard deviation of activity measure by quarter
		matrix diag_q = J(4,2,.)
		forv q = 1/4{
			capture noisily {
				qui reg activity_harmonized_2019  if mktday == 1 & _merge==3 & year==2019 & quarter==`q'
				matrix diag_q[`q',1] = _se[_cons]
				matrix diag_q[`q',2] = e(N)
			}
		}

		// number of observations by month for a market sample of size `samplesize'
		svmat diag_q
		qui su diag_q1 if diag_q1 > 0
		local mean_se = r(mean)
		qui su diag_q2 if diag_q1 > 0
		local N = r(mean)
		// collect estimates of standard error and # of obs
		matrix mean_se[`draw',4] = `mean_se'
		matrix mean_se[`draw',5] = `N'
		
		// run regression to calculate standard deviation of activity measure by year
		matrix diag_y = J(2,2,.)
		forv y=1/2{
			local year=2017+`y'
			capture noisily {
				qui reg activity_measure_norm_2019 if mktday == 1 & _merge==3 & year==`year' 
				matrix diag_y[`y',1]=_se[_cons]
				matrix diag_y[`y',2] = e(N)
			}
		}
		// number of observations by month for a market sample of size `samplesize'
		svmat diag_y
		qui su diag_y1 if diag_y1>0
		local mean_se=r(mean)
		qui su diag_y2 if diag_y1 > 0
		local N = r(mean)
		// collect estimates of standard error and # of obs
		matrix mean_se[`draw',6]= `mean_se'
		matrix mean_se[`draw',7]= `N'
		
		drop _merge diag*
	}
	svmat mean_se
	qui su mean_se2
	local sd_m = r(mean)
	qui su mean_se3
	local obs_m = round(r(mean),1)
	qui su mean_se4
	local sd_q = r(mean)
	qui su mean_se5
	local obs_q = round(r(mean),1)
	qui su mean_se6
	local sd_y = r(mean)
	qui su mean_se7
	local obs_y = round(r(mean),1)
	drop mean_se*
	// perform power calculations for different 
	foreach power in 0.5 0.8 0.9 { // power targets
		foreach alpha in 0.05 0.1{ // significance levels
			local n=`n'+1
			capture noisily{ 
			qui	power twomeans `control_mean', sd(`sd_m') alpha(`alpha') power(`power') n1(`obs_m') n2(`obs_m') 
			
				quietly{
					preserve
						use `tostore', clear
						set obs `n'
						replace samplesize = `samplesize' in `n'
						replace alpha = "`alpha'" in `n'
						replace power = "`power'" in `n'
						replace delta = r(delta) in `n'
						replace interval="month" in `n'
						replace obs_per_interval=`obs_m' in `n'
						tempfile tostore
						save  `tostore'
					restore
				}
			}
			local n=`n'+1
			qui power twomeans `control_mean', sd(`sd_q') alpha(`alpha') power(`power') n1(`obs_q') n2(`obs_q')
			quietly{
				preserve
					use `tostore', clear
					set obs `n'
					replace samplesize = `samplesize' in `n'
					replace alpha = "`alpha'" in `n'
					replace power = "`power'" in `n'
					replace delta = r(delta) in `n'
					replace interval="quarter"  in `n'
					replace obs_per_interval=`obs_q' in `n'
					tempfile tostore
					save  `tostore'
				restore
			}
			local n=`n'+1
			qui power twomeans `control_mean', sd(`sd_y') alpha(`alpha') power(`power') n1(`obs_y') n2(`obs_y')
			quietly{
				preserve
					use `tostore', clear
					set obs `n'
					replace samplesize = `samplesize' in `n'
					replace alpha = "`alpha'" in `n'
					replace power = "`power'" in `n'
					replace delta = r(delta) in `n'
					replace interval="year"  in `n'
					replace obs_per_interval=`obs_y' in `n'
					tempfile tostore
					save  `tostore'
				restore
			}
		}
	}
}

use `tostore', clear
save "temp/tostore.dta", replace

use "temp/tostore.dta", clear
su delta if interval=="year"
local ylim=ceil(log(r(max)))
replace delta=. if delta>32
drop if samplesize>64

gen obs_per_market = obs_per_interval/samplesize
su obs_per_market if interval=="month"
local obs_month =round(r(mean),1)
su obs_per_market if interval=="quarter"
local obs_quarter =round(r(mean),1)
su obs_per_market if interval=="year"
local obs_year =round(r(mean),1)

gen log_delta = log(delta)

gen log_samplesize = log(samplesize)

qui su delta
local max=int(r(max))
local labels -.69314718 "0.5"  0 "1"
forv d=1(1)5{
	local e=2^`d'
	local dd= log(`e')
	local labels `labels' `dd' "`e'"
}
// di `labels'
local xlabels 0 "1"
forv m=1(1)6{
	local mm=2^`m'
	local mmm=log(`mm')
	local xlabels `xlabels' `mmm' "`mm'"
}

local a_goal "0.05"

su log_delta if interval=="month" & alpha=="`a_goal'" & power=="0.5"
local y_m = r(min)
su log_delta if interval=="quarter" & alpha=="`a_goal'" & power=="0.5"
local y_q = r(min)
su log_delta if interval=="year" & alpha=="`a_goal'" & power=="0.5"
local y_y = r(min)

local options  ytitle("") yscale(reverse) xtitle("") ylabel(`labels') xlabel(`xlabels')  yscale(range(0(10)`ylim'))


tw (line log_delta log_samplesize if power=="0.5" & alpha == "`a_goal'" & interval=="month", lc("$color1") lp(shortdash)) ///
	(line log_delta log_samplesize if power=="0.8" & alpha == "`a_goal'" & interval=="month", lc("$color1") lp(dash)) ///
	(line log_delta log_samplesize if power=="0.9" & alpha == "`a_goal'" & interval=="month", lc("$color1") ) ///
	(line log_delta log_samplesize if power=="0.5" & alpha == "`a_goal'" & interval=="quarter", lc("$color2") lp(shortdash)) ///
	(line log_delta log_samplesize if power=="0.8" & alpha == "`a_goal'" & interval=="quarter", lc("$color2") lp(dash)) ///
	(line log_delta log_samplesize if power=="0.9" & alpha == "`a_goal'" & interval=="quarter", lc("$color2")) ///
	(line log_delta log_samplesize if power=="0.5" & alpha == "`a_goal'" & interval=="year", lc("$color3") lp(shortdash)) ///
	(line log_delta log_samplesize if power=="0.8" & alpha == "`a_goal'" & interval=="year", lc("$color3") lp(dash)) ///
	(line log_delta log_samplesize if power=="0.9" & alpha == "`a_goal'" & interval=="year", lc("$color3")) ///
	, legend(order(1 "50%" 2 "80%" 3 "90%") title("Power (with {&alpha}=`a_goal' in two-sided test)", size(medsmall)) row(1) symxsize(*0.75) ring(0) pos(11)) ///
	`options' name(power, replace) xtitle("# of markets in sample") ytitle("Minimum detectable change") ///
	text(2.2 3.5 "Month-on-" "month (`obs_month')", color("$color1") placement(s)) ///
	text(1 3 "Quarter-on-" "quarter (`obs_quarter')", color("$color2") placement(c) bcolor(white) fcolor(white) box bmargin(medsmall)) ///
	text(0.2 1 "Year-on-year" "(Mean # readings per" "market & period: `obs_year')", color("$color3") placement(s)) 
   graph export "graphs/figures6.png", replace height(2000)
