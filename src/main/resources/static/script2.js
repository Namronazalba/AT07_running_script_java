$(document).ready(function() {
    function fetchData() {
        $.ajax({
            url: '/temporary_values',
            type: 'GET',
            dataType: 'text',
            success: function(data) {
                $('#dynamic-data-container2').html(data);
            },
            error: function(xhr, status, error) {
                console.error('Error fetching data:', error);
            }
        });
    }

    setInterval(fetchData, 1000);
});
