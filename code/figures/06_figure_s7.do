global color1 "42 157 143"
global color2 "233 196 106"
global color3 "231 111 81"
global color4 "150 177 137"
global color5 "234 155 94"

use "temp/activityAndWeather_eth.dta", replace

keep if inlist(year,2018,2019)
/*
preserve
	duplicates drop mktid, force
	sample 200, count
	keep mktid
	tempfile tmp
	save `tmp'
restore

merge m:1 mktid using `tmp', keep(3) nogen
*/
tw  (hist activity_harmonized_2018 if mktday==1, frac color("$color1") barw(3.5) bin(100) lw(0)) ///
	, ytitle("") xtitle("") title("Market days", ring(0) pos(1) color("$color1")  size(small)) ///
	 name(up, replace) xlabel(0(100)300,nolabel) xscale(range(-50(50)300)) ylabel(, labsize(small))  graphregion(margin(0 +0 0 +0))
	 
tw  (hist activity_harmonized_2018 if  mktday==0, frac color("$color1*0.5") barw(3.5) bin(100)  lw(0)) ///
	, ytitle("") xtitle("")  title("Non-market days", ring(0) pos(1) color("$color1*0.5")  size(small)) ///
	 name(down, replace) xlabel(0(100)300, labsize(small)) xscale(range(-50(50)300)) ylabel(, labsize(small)) graphregion(margin(0 +0 0 +0))
	 
graph combine up down, l1title("Share of activity measures", size(small)) b1title("Market activity index", size(small)) title("{bf:A}", pos(10)  size(small)) name(a, replace) col(1) iscale(1) graphregion(margin(zero)) 

qui tab mktid
local n=r(r)
matrix coll=J(`n',2,.)
levelsof mktid, local(mkts)
foreach mkt of local mkts{
	di "`mkt'"
	local nn=`nn'+1
	capture noisily{
		ranksum activity_harmonized_2018 if mktid=="`mkt'" , by(mktday) 
		matrix coll[`nn',1] = `nn'
		matrix coll[`nn',2] = r(p)
	}
	
}
capture drop coll*
svmat coll

gen count = 1
gen plevel = .
	replace plevel = 1 if coll2<0.01
	replace plevel = 2 if coll2<0.05 & plevel==.
	replace plevel = 3 if coll2<0.1 & plevel==.
	replace plevel = 4 if coll2>0.1 & plevel==. & coll2!=.

su coll2
replace count=count/r(N) 

graph bar (sum)	count, over(plevel, relabel(1 "<0.01" 2 "<0.05" 3 "<0.1" 4 ">0.1") label(labsize(small)) ) b1title("p-values" , size(small)) bar(1, color("$color3%50") lw(0)) ytitle("Share of markets" " ", size(small)) name(b, replace) yscale(range(0(.2).8)) ylabel(0(0.2).8, labsize(small)) title("{bf:B}", pos(10) size(small)) fxsize(65) graphregion(margin(zero)) ///
text(.85 30 "H{sub:0}: ", placement(e)) ///
text(.85 40 " MAI{sub:md}", color("$color1") placement(e)) ///
text(.85 65 "{sup:d}=",  placement(e)) ///
text(.85 76 "MAI{sub:nmd}", color("$color1%50") placement(e)) 


/*
tw (hist coll2, color("$color1") barw(0.01) bin(100)), ///
xline(0.05, lp(dash) lc(gs6)) title("{bf:B}", pos(10)) name(b, replace) xtitle("p-values MAI{sup:non-market day} {sup:d}= MAI{sup:market day}") ytitle("Density") 
*/
graph combine a b, row(1) iscale(1)
graph export "graphs/figure_s7.png", replace height(2000)