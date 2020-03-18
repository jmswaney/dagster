from collections import namedtuple

from dagster import check
from dagster.core.definitions import (
    CompositeSolidDefinition,
    InputDefinition,
    OutputDefinition,
    PipelineDefinition,
    SolidDefinition,
)
from dagster.core.serdes import whitelist_for_serdes

from .config_types import ConfigFieldSnap, snap_from_field
from .dep_snapshot import (
    DependencyStructureSnapshot,
    build_dep_structure_snapshot_from_icontains_solids,
)


def build_solid_definitions_snapshot(pipeline_def):
    check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)
    return SolidDefinitionsSnapshot(
        solid_def_snaps=[
            build_solid_def_snap(solid_def)
            for solid_def in pipeline_def.all_solid_defs
            if isinstance(solid_def, SolidDefinition)
        ],
        composite_solid_def_snaps=[
            build_composite_solid_def_snap(solid_def)
            for solid_def in pipeline_def.all_solid_defs
            if isinstance(solid_def, CompositeSolidDefinition)
        ],
    )


class SolidDefinitionsSnapshot(
    namedtuple('_SolidDefinitionsSnapshot', 'solid_def_snaps composite_solid_def_snaps')
):
    def __new__(cls, solid_def_snaps, composite_solid_def_snaps):
        return super(SolidDefinitionsSnapshot, cls).__new__(
            cls,
            solid_def_snaps=check.list_param(
                solid_def_snaps, 'solid_def_snaps', of_type=SolidDefSnap
            ),
            composite_solid_def_snaps=check.list_param(
                composite_solid_def_snaps,
                'composite_solid_def_snaps',
                of_type=CompositeSolidDefSnap,
            ),
        )


def build_input_def_snap(input_def):
    check.inst_param(input_def, 'input_def', InputDefinition)
    return InputDefSnap(
        name=input_def.name,
        dagster_type_key=input_def.dagster_type.key,
        description=input_def.description,
    )


def build_output_def_snap(output_def):
    check.inst_param(output_def, 'output_def', OutputDefinition)
    return OutputDefSnap(
        name=output_def.name,
        dagster_type_key=output_def.dagster_type.key,
        description=output_def.description,
        is_required=output_def.is_required,
    )


def build_composite_solid_def_snap(comp_solid_def):
    check.inst_param(comp_solid_def, 'comp_solid_def', CompositeSolidDefinition)
    return CompositeSolidDefSnap(
        name=comp_solid_def.name,
        input_def_snaps=list(map(build_input_def_snap, comp_solid_def.input_defs)),
        output_def_snaps=list(map(build_output_def_snap, comp_solid_def.output_defs)),
        description=comp_solid_def.description,
        tags=comp_solid_def.tags,
        positional_inputs=comp_solid_def.positional_inputs,
        required_resource_keys=list(comp_solid_def.required_resource_keys),
        config_field_snap=snap_from_field('config', comp_solid_def.config_mapping.config_field)
        if comp_solid_def.config_mapping
        else None,
        dep_structure_snapshot=build_dep_structure_snapshot_from_icontains_solids(comp_solid_def),
    )


def build_solid_def_snap(solid_def):
    check.inst_param(solid_def, 'solid_def', SolidDefinition)
    return SolidDefSnap(
        name=solid_def.name,
        input_def_snaps=list(map(build_input_def_snap, solid_def.input_defs)),
        output_def_snaps=list(map(build_output_def_snap, solid_def.output_defs)),
        description=solid_def.description,
        tags=solid_def.tags,
        positional_inputs=solid_def.positional_inputs,
        required_resource_keys=list(solid_def.required_resource_keys),
        config_field_snap=snap_from_field('config', solid_def.config_field)
        if solid_def.config_field
        else None,
    )


# This and _check_solid_def_header_args helps a defacto mixin for
# CompositeSolidDefSnap and SolidDefSnap. Inheritance is quick difficult
# and counterintuitive in namedtuple land, so went with this scheme instead.
SOLID_DEF_HEADER_PROPS = (
    'name input_def_snaps output_def_snaps description tags positional_inputs '
    'required_resource_keys config_field_snap'
)


def _check_solid_def_header_args(
    name,
    input_def_snaps,
    output_def_snaps,
    description,
    tags,
    positional_inputs,
    required_resource_keys,
    config_field_snap,
):
    return dict(
        name=check.str_param(name, 'name'),
        input_def_snaps=check.list_param(input_def_snaps, 'input_def_snaps', InputDefSnap),
        output_def_snaps=check.list_param(output_def_snaps, 'output_def_snaps', OutputDefSnap),
        description=check.opt_str_param(description, 'description'),
        tags=check.dict_param(tags, 'tags'),  # validate using validate_tags?
        positional_inputs=check.list_param(positional_inputs, 'positional_inputs', str),
        required_resource_keys=check.list_param(
            required_resource_keys, 'required_resource_keys', str
        ),
        config_field_snap=check.opt_inst_param(
            config_field_snap, 'config_field_snap', ConfigFieldSnap
        ),
    )


@whitelist_for_serdes
class CompositeSolidDefSnap(
    namedtuple('_CompositeSolidDefSnap', SOLID_DEF_HEADER_PROPS + ' dep_structure_snapshot')
):
    def __new__(
        cls,
        name,
        input_def_snaps,
        output_def_snaps,
        config_field_snap,
        description,
        tags,
        positional_inputs,
        required_resource_keys,
        dep_structure_snapshot,
    ):
        return super(CompositeSolidDefSnap, cls).__new__(
            cls,
            dep_structure_snapshot=check.inst_param(
                dep_structure_snapshot, 'dep_structure_snapshot', DependencyStructureSnapshot
            ),
            **_check_solid_def_header_args(
                name,
                input_def_snaps,
                output_def_snaps,
                description,
                tags,
                positional_inputs,
                required_resource_keys,
                config_field_snap,
            ),
        )


@whitelist_for_serdes
class SolidDefSnap(namedtuple('_SolidDefMeta', SOLID_DEF_HEADER_PROPS)):
    def __new__(
        cls,
        name,
        input_def_snaps,
        output_def_snaps,
        description,
        tags,
        positional_inputs,
        config_field_snap,
        required_resource_keys,
    ):
        return super(SolidDefSnap, cls).__new__(
            cls,
            **_check_solid_def_header_args(
                name,
                input_def_snaps,
                output_def_snaps,
                description,
                tags,
                positional_inputs,
                required_resource_keys,
                config_field_snap,
            )
        )

    def get_input_snap(self, name):
        check.str_param(name, 'name')
        for inp in self.input_def_snaps:
            if inp.name == name:
                return inp

        check.failed('Could not find input ' + name)

    def get_output_snap(self, name):
        check.str_param(name, 'name')
        for out in self.output_def_snaps:
            if out.name == name:
                return out

        check.failed('Could not find output ' + name)


ISolidDefSnap = (CompositeSolidDefSnap, SolidDefSnap)


@whitelist_for_serdes
class InputDefSnap(namedtuple('_InputDefSnap', 'name dagster_type_key description')):
    def __new__(cls, name, dagster_type_key, description):
        return super(InputDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type_key=check.str_param(dagster_type_key, 'dagster_type_key'),
            description=check.opt_str_param(description, 'description'),
        )


@whitelist_for_serdes
class OutputDefSnap(namedtuple('_OutputDefSnap', 'name dagster_type_key description is_required')):
    def __new__(cls, name, dagster_type_key, description, is_required):
        return super(OutputDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, 'name'),
            dagster_type_key=check.str_param(dagster_type_key, 'dagster_type_key'),
            description=check.opt_str_param(description, 'description'),
            is_required=check.bool_param(is_required, 'is_required'),
        )
