julia:
	julia --project=.
pkg_update:
	julia --project=. -e 'using Pkg; Pkg.instantiate(); Pkg.update();'
