# Docker process

1. `docker build -t 52north/sos-solar-postgres:<version> .`
1. `docker push 52north/sos-solar-postgres:<version>`

# SOS Solar Irradiance Example Database

This database contains in-situ data for Solar Irrandiance as well
as data derived by several models. The data is located at a station
in [Signes, France](https://www.google.de/maps/search/signes/@43.2760265,5.7793974,12z,),
and contains data for the first two weeks of September 2015.

The provided data is an excerpt of the database provided on
[http://webservice-energy.org/](http://webservice-energy.org/). This platform is
an effort carried out by the center
[Observation, Impacts, Energy](http://www.oie.mines-paristech.fr/Accueil/) (O.I.E.)
of [MINES ParisTech / ARMINES](http://www.mines-paristech.fr/) and also from the
[SoDa](http://www.soda-pro.com/) Team toward the Energy and Environmental Community.

## Acknowledgement

The data contained in this sample database has been kindly provided by
[Solaïs](http://www.solais.fr/en/).

[![solais](solais.png)](http://www.solais.fr/en/)

Addtionally, we thank
[Mines ParisTech](http://www.mines-paristech.fr/)
(i.e. [Lionel Menard](http://www.mines-paristech.fr/Services/Annuaire/lionel-menard))
for supporting the collection of the sample data.

[![mines-paristech](mines-paristech.png)](http://www.mines-paristech.fr/)
