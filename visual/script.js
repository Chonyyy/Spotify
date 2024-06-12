$(document).ready(function() {
    // Simular carga de datos para las secciones "Featured" y "Popular"
    cargarDatosSeccion("#featured", "Datos destacados");
    cargarDatosSeccion("#popular", "Datos populares");

    function cargarDatosSeccion(seccion, datos) {
        // Simulación de carga de datos
        setTimeout(function() {
            $(seccion).append("<p>" + datos + "</p>");
        }, 1000); // Tiempo de espera simulado (1 segundo)
    }


    $(".featured-item, .popular-item").on("click", function() {
        var audioSrc = $(this).data("audio");
        $("#audio-player").attr("src", audioSrc);
        $("#audio-player")[0].play();
    });


    $('#search').on('input', function() {
        var query = $(this).val().toLowerCase();
        $('.featured-item, .popular-item').each(function() {
            var title = $(this).find('h3').text().toLowerCase();
            if (title.includes(query)) {
                $(this).show();
            } else {
                $(this).hide();
            }
        });
    });
});

