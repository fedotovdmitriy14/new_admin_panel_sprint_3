from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, PersonFilmwork, Person


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name', 'id')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    list_filter = ('full_name',)
    search_fields = ('full_name', 'id')


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type', 'rating',)
    search_fields = ('title', 'description', 'id')
