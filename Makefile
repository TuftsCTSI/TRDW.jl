notebook:
	julia --project -e 'using Pkg; Pkg.instantiate(); using Pluto; cd(".."); Pluto.run()'
update:
	cd .. && git submodule update --remote --merge
pkg_update:
	julia --project=. -e 'using Pkg; Pkg.instantiate(); Pkg.update();'
