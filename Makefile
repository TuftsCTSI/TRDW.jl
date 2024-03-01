julia:
	julia --project=. -e "using Pkg; Pkg.instantiate(); using FunSQL, Revise; using TRDW;" -i

pkg_update:
	julia --project=. -e 'using Pkg; Pkg.instantiate(); Pkg.update();'
