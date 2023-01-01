from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import Filmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        return Filmwork.objects.all().prefetch_related(
            'genres',
            'person',
        ).order_by('id').values(
            'id',
            'title',
            'description',
            'creation_date',
            'rating',
            'type',
        ).annotate(
            genres=ArrayAgg(
                'genres__name',
                distinct=True,
            ),
        ).annotate(
            actors=ArrayAgg(
                'personfilmwork__person__full_name',
                filter=Q(personfilmwork__role='actor'),
                distinct=True,
            ),
        ).annotate(
            directors=ArrayAgg(
                'personfilmwork__person__full_name',
                filter=Q(personfilmwork__role='director'),
                distinct=True,
            ),
        ).annotate(
            writers=ArrayAgg(
                'personfilmwork__person__full_name',
                filter=Q(personfilmwork__role='writer'),
                distinct=True,
            ),
        )

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    paginate_by = 50
    page_number = 1

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'page': self.page_number,
            'results': list(queryset),
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    def get_context_data(self, **kwargs):
        pk = self.kwargs.get(self.pk_url_kwarg)
        queryset = self.get_queryset().get(id=pk)
        return queryset
