$(document).ready(function() {
    function fetchData() {
        $.ajax({
            url: '/dynamic_data',
            type: 'GET',
            dataType: 'text',
            success: function(data) {
                $('#dynamic-data-container').html(data);
            },
            error: function(xhr, status, error) {
                console.error('Error fetching data:', error);
            }
        });
    }

    setInterval(fetchData, 1000);
});
